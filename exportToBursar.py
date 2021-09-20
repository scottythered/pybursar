import jobUtils
import bursarUtils
import moneyMath
import fileServices
import mongoPortal
from logger import log
from tqdm import tqdm
from datetime import date
from datetime import datetime
import json
import os
from auth_puller import auth_puller
from slackMsg import slacker


def finderator(match_point, match_value, dict):
    finder = list(filter(lambda thing: thing[match_point] == match_value, dict))
    if len(finder) == 1:
        return finder[0]
    else:
        raise ValueError(f"finderator expected one result, got {len(finder)} instead")


def exportFileNameHelper(isCharge, exportId, version):
    # LIB_charges.dat.YYYYMMDD.[export#].[1|2]  1: charges,  2: reversals
    fnPrefix = "LIB_charges.dat."
    now = mongoPortal.right_now()
    # expects version to start from 1
    if version <= 0:
        raise ValueError("version must be >= 1")
    else:
        # charge files have odd version numbers and reversals have even version
        # numbers. charges follow the sequence 1, 3, 5, ...
        # while reverals follow sequence 2, 4, 6, ...
        if isCharge:
            bumper = (version * 2) - 1
        else:
            bumper = (version * 2) - 0
    return f"{fnPrefix}{now.strftime('%Y%m%d')}.{exportId}.{bumper}"


def getChargesFileName(exportId):
    return exportFileNameHelper(True, exportId, 1)


def getReversalsFileName(exportId):
    return exportFileNameHelper(False, exportId, 1)


def applyAlmaImportsToBurs():
    # Apply credits and waives or new fines from alma: this algorithm will create
    # new entries in TransBurs with newBursExp = true based on TransAlma entries
    # with newAlmaImp = true.  At the end, there should be no TransAlma with
    # newAlmaImp = true.
    # - assumption: there are no outstanding payments
    # - if abs(amtA) > origAmt or abs(amtB) > origAmt then Error!
    # - if amtA < 0 && amtA-amtB=diff < 0, then new Credit with amt = diff
    # - if amtA >= 0 && amtA-amtB=diff < 0, then new Waive with amt = diff
    # - if diff == 0, then do nothing.
    # - if diff > 0 and amtB != 0 then error!
    # - otherwise, => amtB == 0 and amtA > 0 and diff > 0 => new fine.
    # - if amtA <=0 && amtA-amtB=diff > 0, then new Fine
    # - should only create new entries in TransBurs and set newAlmaImp to false.
    # - ideally, we would query Bursar as we were doing this to ensure everything ok.
    # - Payments are a special case: when they come from Alma, we assume they
    # already exist in Bursar, and therefore TransBurs entires are immediately
    log.info("Export service started: applying Alma imports to bursar")
    # FineAndDiff = { id: number; diff: number, amtA: number, amtB: number }

    posDiffNewFine = []
    zeroDiff = []
    negDiffNewCredit = []
    negDiffNewWaive = []
    miscError = []

    fineIds = mongoPortal.getNewAlmaImps()

    if fineIds is not None:
        log.info(f"Got {len(fineIds)} fine ids from transAlma where newAlmaImp = true:")
        log.info(f"{fineIds}")

        for fine_id in tqdm(fineIds):
            f = mongoPortal.select_fines_ta_tb_bMD_on_FineId(fine_id)

            log.debug(f"processing fine: {f['id']}")
            amtA = moneyMath.correct(sum([trans["amt"] for trans in f["trans_alma"]]))
            if len(f["trans_burs"]) == 0:
                amtB = 0
            else:
                amtB = moneyMath.correct(sum([burs["amt"] for burs in f["trans_burs"]]))
            diff = moneyMath.correct(amtA - amtB)
            detail = {"id": f["id"], "amtA": amtA, "amtB": amtB, "diff": diff}

            if moneyMath.isGt(abs(amtA), f["origAmtAlma"]) or moneyMath.isGt(
                abs(amtB), f["origAmtAlma"]
            ):
                miscError.append(detail)
                msg = f"Absolute value of transactions cannot exceed original fine amt {f['origAmtAlma']} for detail {detail}"
                log.error(msg)
                raise ValueError(msg)
            elif (moneyMath.isGt(diff, 0) == True) and (
                moneyMath.isEqual(amtB, 0) == False
            ):
                miscError.append(detail)
                msg = f"amtA > 0 which implies new fine, but amtB != 0 for detail {detail}"
                log.error(msg)
                raise ValueError(msg)
            elif moneyMath.isGt(diff, 0) and f["isSierraFine"]:
                miscError.append(detail)
                msg = f"Cannot send new Sierra fines to Bursar!: {detail}"
                log.error(msg)
                raise ValueError(msg)
            elif moneyMath.isEqual(diff, 0):
                zeroDiff.append(detail)
                log.debug(
                    f"Fine {f['id']} amtA {amtA} == amtB {amtB}, so nothing to send to Bursar."
                )
            elif moneyMath.isGt(diff, 0):
                if len(f["burs_fine_md"]) != 1:
                    miscError.append(detail)
                    msg = f"Want to send a new Fine transaction to TransBurs for fine {f['id']}, but fine['burs_fine_md']'s length is {len(f['burs_fine_md'])}, not 1 as expected."
                    log.error(msg)
                    raise ValueError(msg)
                else:
                    md = f["burs_fine_md"][0]
                    tb = {}
                    tb["amt"] = diff
                    tb["type"] = "Fine"
                    tb["bursFineId"] = md["bursFineId"]
                    tb["newBursImp"] = False
                    # indicates this needs exporting to Bursar:
                    tb["newBursExp"] = True
                    if "trans_burs" not in f:
                        f["trans_burs"] = []
                    f["trans_burs"].append(tb)
                    posDiffNewFine.append(detail)
                    log.debug(f"Sending a new fine to Bursar for amount {diff}.")
            elif moneyMath.isLt(diff, 0):
                # express the credit/waive amount as a positive number
                credit = -diff
                if moneyMath.isGtEqual(amtA, 0):
                    type = "Waive"
                else:
                    type = "Credit"

                if len(f["burs_fine_md"]) > 1:
                    log.debug(f"Applying {type} to two-part Sierra fine: {detail}")

                origTbAmts = [
                    tb["amt"] for tb in f["trans_burs"] if tb["type"] == "Fine"
                ]
                if len(origTbAmts) != len(f["burs_fine_md"]):
                    miscError.append(detail)
                    msg = f"Number of TransBurs original Fine amts do not equal number of BursFineMd: origTbAmts length ({len(origTbAmts)}) != f['burs_fine_md'] length ({len(f['burs_fine_md'])}) for fine {f['id']}"
                    log.error(msg)
                    raise ValueError(msg)
                else:
                    targets = []
                    for bursFineMd in f["burs_fine_md"]:
                        index = f["burs_fine_md"].index(bursFineMd)
                        md = bursFineMd
                        owe = moneyMath.correct(
                            sum(
                                [
                                    t["amt"]
                                    for t in f["trans_burs"]
                                    if t["bursFineId"] == md["bursFineId"]
                                ]
                            )
                        )
                        targets.append({"curAmt": owe, "origAmt": origTbAmts[index]})
                    allocs = moneyMath.distributeCreditBurs(
                        credit, targets, (type == "Credit")
                    )
                    log.debug(f"allocs: {allocs}")
                    if moneyMath.isGt(allocs["remCred"], 0):
                        miscError.append(detail)
                        msg = f"for fine {f['id']}, was not able to allocate {type} of {credit}. targets: {targets}\nallocs: {allocs}"
                        log.error(msg)
                        raise ValueError(msg)
                    else:
                        for bursFineMd in f["burs_fine_md"]:
                            index = f["burs_fine_md"].index(bursFineMd)
                            if moneyMath.isEqual(allocs["creds"][index], 0):
                                pass
                            else:
                                md = bursFineMd
                                tb = {}
                                tb["amt"] = -(allocs["creds"][index])
                                tb["type"] = type
                                tb["bursFineId"] = md["bursFineId"]
                                tb["newBursImp"] = False
                                # indicates this needs exporting to Bursar:
                                tb["newBursExp"] = True
                                if f["trans_burs"] is not None:
                                    f["trans_burs"].append(tb)
                                else:
                                    f["trans_burs"] = []
                                    f["trans_burs"].append(tb)
                        if type == "Credit":
                            negDiffNewCredit.append(detail)
                        elif type == "Waive":
                            negDiffNewWaive.append(detail)

            if f["id"] == 2372:
                log.debug("An interesting case with diff != 0!")

            updateTas = [ta for ta in f["trans_alma"] if ta["newAlmaImp"] == True]
            if len(updateTas) == 0:
                pass
            else:
                for ta in updateTas:
                    ### taRep.persist(updateTas); update with ta["newAlmaImp"] = False
                    mongoPortal.set_newAlmaImp_to_false(ta["id"])
            ### await fRep.persist(f); -- persist new trans_bursars in fines
            if f["trans_burs"] is not None:
                for tb in f["trans_burs"]:
                    if "id" not in tb:
                        mongoPortal.insertBursTrans(tb, f["id"])

    r = {
        "newFines": len(posDiffNewFine),
        "zeroDiffs": len(zeroDiff),
        "newWaives": len(negDiffNewWaive),
        "newCredits": len(negDiffNewCredit),
        "miscErrors": len(miscError),
    }

    log.info(f"ApplyAlmaImportsToBurs stats: {r}")
    if r["newFines"] > 0:
        log.info(f"New Fines: {','.join([str(fine['id']) for fine in posDiffNewFine])}")
    if r["zeroDiffs"] > 0:
        log.info(
            f"Zero-Differences Not Sent to Bursar: {','.join([str(fine['id']) for fine in zeroDiff])}"
        )
    if r["newWaives"] > 0:
        log.info(
            f"New Waives: {','.join([str(fine['id']) for fine in negDiffNewWaive])}"
        )
    if r["newCredits"] > 0:
        log.info(
            f"New Credits: {','.join([str(fine['id']) for fine in negDiffNewCredit])}"
        )
    if r["miscErrors"] > 0:
        log.info(f"Misc. Errors: {','.join([str(fine['id']) for fine in miscError])}")

    return True


def computeBursExport():
    log.debug("In ToBursarService.computeBursExport")
    # Get trans_burs_id , fine_id  pairs
    ids = mongoPortal.get_tb_fine_id_pairs()
    if len(ids) == 0:
        log.info(
            f"Found no fine/transBurs id pairs for transBurs where newBursExp = true..."
        )
        return True, None, None
    else:
        new_job_number = jobUtils.startNewJob("BursExport")
        new_bursar_export_id_number = mongoPortal.insertBursExportRow()
        log.info(f"bursar_export_id: {new_bursar_export_id_number}")
        log.debug(
            f"Found {len(ids)} fine/transBurs id pairs from transBurs where newBursExp = true..."
        )
        numExported = 0
        for id_pair in ids:
            log.debug(f"processing tb {id_pair['tbId']}, md {id_pair['mdId']}")
            bet = {}
            bet["transBurs"] = id_pair["tbId"]
            bet["bursExport"] = new_bursar_export_id_number
            bet["bursFineMd"] = id_pair["mdId"]
            ### await betRep.persist(bet) -- add new row to burs_export_trans
            mongoPortal.insertBursExportTransRow(bet)
            ###await tbRep.persist(tb) -- update tb["newBursExp"] = false on trans_burs
            mongoPortal.set_newBursExp_to_false(id_pair["tbId"])
            numExported += 1

        log.info(f"Added {str(numExported)} to burs_export_trans")
        return True, new_bursar_export_id_number, new_job_number


def generateBursExportFile(bursExportId, updateSentDates, newJobIdNumber):
    """Based on BursExport id, write all BursExportTrans items, updating the
    sent date for all of them.
    """
    if bursExportId is None and newJobIdNumber is None:
        log.info("Nothing to export to Bursar!")
        return True, 0
    else:
        paths = auth_puller("auth.json", "paths")
        bursExportDir = os.path.join(paths["local_path"], paths["bursExportArchive"])

        log.debug("In generateBursExportFile")

        fetch_dict = mongoPortal.findMany(
            "burs_export_trans", "bursExport", bursExportId
        )

        log.debug("Generating Bursar export")

        if updateSentDates:
            log.debug("Set to overwrite sentDate-related info")
        else:
            log.debug("Not set to overwrite sentDate-related info")

        tc_date = date.today()
        sentM = datetime.today()
        dueM = bursarUtils.dueDateCloner(sentM)

        tbRep = mongoPortal.getAll("trans_burs")
        mdRep = mongoPortal.getAll("fine_mds")
        fRep = mongoPortal.getAll("fines")

        bets = []
        for entry in fetch_dict:
            temp_bet = {}
            temp_bet["trans_burs"] = finderator("id", entry["transBurs"], tbRep)
            temp_bet["burs_fine_md"] = finderator("id", entry["bursFineMd"], mdRep)
            temp_bet["fine"] = finderator("id", temp_bet["trans_burs"]["fine"], fRep)
            bets.append(temp_bet)

        numLines = len(bets)
        charges = sum([1 if (c["trans_burs"]["amt"] > 0) else 0 for c in bets])
        reversals = numLines - charges
        log.info(f"charges: {charges}, reversals: {reversals}")

        if charges > 0 and updateSentDates == True:
            be_chargesFilename = getChargesFileName(bursExportId)
        else:
            be_chargesFilename = None

        if reversals > 0 and updateSentDates == True:
            be_reversalsFilename = getReversalsFileName(bursExportId)
        else:
            be_reversalsFilename = None

        be_sent = mongoPortal.right_now()
        be_numSent = numLines

        chrgsPath = None
        revsPath = None
        error_msg = "Export directory path not found"
        if fileServices.pathExists(bursExportDir):
            if charges > 0:
                if fileServices.pathExists(bursExportDir):
                    chrgsPath = os.path.join(bursExportDir, be_chargesFilename)
                    log.debug(f"chrgsPath: {chrgsPath}")
            if reversals > 0:
                if fileServices.pathExists(bursExportDir):
                    revsPath = os.path.join(bursExportDir, be_reversalsFilename)
                    log.debug(f"chrgsPath: {revsPath}")
        else:
            log.error(error_msg)
            raise OSError(error_msg)

        updateMds = []
        chrgs_file = []
        revsFd_file = []

        for bet in bets:
            f = bet["fine"]
            tb = bet["trans_burs"]
            usd = updateSentDates
            bill = sentM
            due = dueM
            md = bet["burs_fine_md"]
            if moneyMath.isLt(tb["amt"], 0):
                if md["billingDate"] is None:
                    raise ValueError(
                        f"Billing date must already exist for a reveral of fine {tb['bursFineId']}"
                    )
                if md["fineDueDate"] is None:
                    raise ValueError(
                        f"Fine due date must already exist for a reveral of fine {tb['bursFineId']}"
                    )
                if md["termCode"] is None:
                    raise ValueError(
                        f"Term code must already exist for a reveral of fine {tb['bursFineId']}"
                    )
            if md["billingDate"] is None or (
                usd is True and moneyMath.isGtEqual(tb["amt"], 0)
            ):
                md["billingDate"] = bill
            if md["fineDueDate"] is None or (
                usd is True and moneyMath.isGtEqual(tb["amt"], 0)
            ):
                md["fineDueDate"] = due
            if md["termCode"] is None or (
                usd is True and moneyMath.isGtEqual(tb["amt"], 0)
            ):
                md["termCode"] = bursarUtils.billingDateToTermCode(tc_date)
            updateMds.append(md)

            log.debug(f"Working on bet: {bet}")
            log.debug(f"with associated md: {md}")

            # now construct each line for the Oasis files
            if moneyMath.isLt(tb["amt"], 0):
                line = bursarUtils.bursTransToOasisLine(
                    f["almaPatronId"],
                    md["chargeAcc"],
                    tb["amt"],
                    None,
                    None,
                    md["termCode"],
                    tb["bursFineId"],
                )
            else:
                line = bursarUtils.bursTransToOasisLine(
                    f["almaPatronId"],
                    md["chargeAcc"],
                    tb["amt"],
                    md["fineDueDate"],
                    md["billingDate"],
                    md["termCode"],
                    tb["bursFineId"],
                )
            if moneyMath.isGt(tb["amt"], 0):
                chrgs_file.append(line)
            else:
                revsFd_file.append(line)

        if len(chrgs_file) > 0:
            with open(chrgsPath, "w") as f:
                for line in chrgs_file:
                    f.write(line + "\n")

        if len(revsFd_file) > 0:
            with open(revsPath, "w") as f:
                for line in revsFd_file:
                    f.write(line + "\n")

        # update burs_export row
        mongoPortal.updateBursExportRow(
            be_chargesFilename, be_reversalsFilename, be_numSent, bursExportId
        )

        # update job row
        jobUtils.jobWin(newJobIdNumber)

        # update MDs
        mdRep_redux = mongoPortal.getAll("fine_mds")
        for md in updateMds:
            md_match = finderator("id", md["id"], mdRep_redux)
            if md != md_match:
                now = mongoPortal.right_now()
                fine_mds = mongoPortal.arise_mongo("fine_mds")
                new_values = {
                    "$set": {
                        "billingDate": md["billingDate"],
                        "fineDueDate": md["fineDueDate"],
                        "termCode": md["termCode"],
                        "updated": now,
                    }
                }
                query = {"id": md["id"]}
                updated = fine_mds.update_one(query, new_values)
                if updated.modified_count != 1 and updated.matched_count != 1:
                    log.error(
                        f"Updated more rows than anticipated on fine md {md['id']}"
                    )

        # preserve number of charges and reversals sent for Slack messaging
        export_dict = {"charges": charges, "reversals": reversals}

        return True, be_numSent, export_dict


def sendBursExportFile():
    # 1) get bursarExport ID with no sent date
    # 2) See which filenames are attached to that row
    # 3) SFTP those file(s) to Bursar
    # 4) Copy file(s) to backup folder
    bursar_export_job = mongoPortal.findLatestOneNull("burs_exports", "sent_to_bursar")
    if bursar_export_job is None:
        msg = "No unsent Bursar Export jobs found, so nothing to send to Bursar."
        log.info(msg)
        slacker("SFTP Process", msg)
    else:
        log.info(
            f"In sendBursExportFile, found unsent files for bursExport {bursar_export_job['id']}"
        )
        log.info("Processing charges file...")
        charges = fileServices.bursarFileProcess(bursar_export_job["chargesFilename"])
        log.info("Processing reversals file...")
        reversals = fileServices.bursarFileProcess(
            bursar_export_job["reversalsFilename"]
        )
        if charges is True and reversals is True:
            now = mongoPortal.right_now()
            burs_exports = mongoPortal.arise_mongo("burs_exports")
            new_values = {"$set": {"sent_to_bursar": now, "updated": now}}
            query = {"id": bursar_export_job["id"]}
            result = burs_exports.update_one(query, new_values)
            if result.modified_count == 1 and result.matched_count == 1:
                files_that_were_sent = []
                if bursar_export_job["chargesFilename"] is not None:
                    files_that_were_sent.append(bursar_export_job["chargesFilename"])
                if bursar_export_job["reversalsFilename"] is not None:
                    files_that_were_sent.append(bursar_export_job["reversalsFilename"])
                joined = " & ".join(files_that_were_sent)
                msg = f"{joined} successfully transfered to Bursar via SFTP."
                log.info(msg)
                slacker("SFTP Process", msg)
        else:
            msg = "Expected to SFTP charges file and/or reversals file, but something went wrong, check the log"
            log.error(msg)
            slacker("SFTP Process", msg)
            sys.exit(-1)


def bursarExport():
    applied = applyAlmaImportsToBurs()
    if applied:
        computed, new_bursar_export_id, new_job_id = computeBursExport()
        if computed:
            generated, sent_no, sent_dict = generateBursExportFile(
                new_bursar_export_id, True, new_job_id
            )
            if generated:
                if sent_no == 0:
                    msg = f"Export complete, no files generated because there was nothing to send to bursar."
                    log.info(msg)
                    slacker("Export Process", msg)
                    return False
                else:
                    msg = f"Export complete, {sent_no} transactions exported ({sent_dict['charges']} charges and {sent_dict['reversals']} reversals) and ready to be sent to bursar."
                    log.info(msg)
                    slacker("Export Process", msg)
                    return True
            else:
                msg = "Failed to generate bursar export files."
                log.error(msg)
                slacker("Export Process", msg)
                sys.exit(-1)
        else:
            msg = "Failed to compute bursar export process."
            log.error(msg)
            slacker("Export Process", msg)
            sys.exit(-1)
