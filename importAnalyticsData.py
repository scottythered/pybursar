import pandas as pd
import json
from datetime import datetime
import re
import math
from tqdm import tqdm
import sfLookup
import almaAPI
import moneyMath
from logger import log
import jobUtils
import mongoPortal
from slackMsg import slacker
import sys


def parseSierraNote(note):
    sierraTsForm = "%Y-%m-%d %H:%M:%S"
    barcodeReg = r"^(\d|[A-Z]){5,12}$"
    if isinstance(note, str):
        if note == "":
            return None
        else:
            fields = note.split(" | ")
            split_fields = [n.strip() for n in fields]
            if len(split_fields) < 3:
                return None
            else:
                sd = {}
                for field in split_fields:
                    i = field.index(":")
                    if i <= 0:
                        return None
                    else:
                        name = (field[0:i]).strip()
                        val = (field[i + 2 :]).strip()
                        if name == "INVOICE":
                            sd["invoice"] = val
                        elif name == "ITEM FEE":
                            sd["itemFee"] = float(val)
                        elif name == "PROCESSING FEE":
                            sd["processingFee"] = float(val)
                        elif name == "BILLING FEE":
                            sd["billingFee"] = float(val)
                        elif name == "FINE COMMENT":
                            sd["fineComment"] = val
                        elif name == "OUT DATE":
                            # eg 2016-11-28 21:04:11 assumed shown in MST
                            sd["outDate"] = datetime.strptime(val, sierraTsForm)
                        elif name == "DUE DATE":
                            sd["dueDate"] = datetime.strptime(val, sierraTsForm)
                        elif name == "RETURNED DATE":
                            sd["returnedDate"] = datetime.strptime(val, sierraTsForm)
                        elif name == "ITEM TITLE":
                            sd["itemTitle"] = val
                        elif name == "DELETED INFO":
                            sd["deletedInfo"] = val
                        elif name == "BARCODE":
                            match = re.fullmatch(barcodeReg, (val).strip())
                            if match:
                                sd["barcode"] = val
                            else:
                                sd["barcode"] = ""
                        else:
                            msg = f"UNEXPECTED FIELD: {name} with val {val}"
                            errorCaught("Parse Sierra Note", msg)

                if sd["invoice"] == "336851" or sd["invoice"] == "336863":
                    sd["itemFee"] += sd["billingFee"]
                    sd["billingFee"] = 0

                if sd["invoice"] == "344897":
                    sd["billingFee"] = 5

                if sd["invoice"] == "332021":
                    sd["itemFee"] = 16.89

                return sd
    else:
        if note is None:
            return None


def tableToTrans(table):
    transTypes = ["Fine", "Payment", "Waive", "Credit"]
    log.info("Transforming transactions data")
    trans_dict = table.to_dict(orient="records")
    trans = []
    for transaction in trans_dict:
        if moneyMath.ignoreFine(transaction["Fine Fee Id"]):
            pass
        else:
            temp_trans = {}
            temp_trans["almaFineId"] = transaction["Fine Fee Id"]
            temp_trans["almaTransId"] = transaction["Fine Fee Transaction Id"]
            temp_trans["almaCreated"] = transaction[
                "Fine Fee Transaction Creation Date"
            ]
            if transaction["Fine FeeTransaction Type"] in transTypes:
                temp_trans["type"] = transaction["Fine FeeTransaction Type"]
            else:
                msg = f"Unexpected Alma Transaction type {transaction['Fine FeeTransaction Type']}"
                errorCaught("Table To Trans", msg)
            temp_trans["almaNote"] = transaction["Transaction Note"]
            if (
                temp_trans["type"] == "Payment"
                or temp_trans["type"] == "Waive"
                or temp_trans["type"] == "Credit"
            ):
                temp_trans["amt"] = -(
                    abs(moneyMath.correct(float(transaction["Transaction Amount"])))
                )
            elif temp_trans["type"] == "Fine":
                temp_trans["amt"] = moneyMath.correct(
                    float(transaction["Transaction Amount"])
                )
            trans.append(temp_trans)
    return trans


def tableToFines(table):
    finesLookupDict = sfLookup.getFinesLookup()
    item_date_format = "%Y-%m-%dT%H:%M:%S"
    log.debug("In tableToFines")
    if len(table) == 0:
        return None
    else:
        problemFines = []
        fines = []
        fineTypes = [
            "Lost item replacement fee",
            "Lost item process fee",
            "Overdue fine",
            "Recalled Overdue fine",
            "Card renewal",
            "Damaged item fee",
            "Other",
            "Registration fee",
            "Credit",
        ]
        fines_dict = table.to_dict(orient="records")
        for fine_entry in tqdm(fines_dict):
            if moneyMath.ignoreFine(fine_entry["Fine Fee Id"]):
                pass
            else:
                temp_fine = {}
                temp_fine["item_detail"] = []
                temp_fine["almaPatronId"] = fine_entry["Primary Identifier"]
                temp_fine["rootAlmaFineId"] = fine_entry["Fine Fee Id"]
                temp_fine["almaCreated"] = datetime.strptime(
                    fine_entry["Fine Fee Creation Date"], item_date_format
                )
                temp_fine["almaUpdated"] = datetime.strptime(
                    fine_entry["Status Date"], item_date_format
                )
                temp_fine["type"] = fine_entry["Fine Fee Type"]
                if fine_entry["Fine Fee Status"] == "Closed":
                    temp_fine["isClosedInAlma"] = True
                else:
                    temp_fine["isClosedInAlma"] = False
                if fine_entry["Item Loan Id"] and fine_entry["Item Loan Id"] != "-1":
                    temp_fine["almaLoanId"] = fine_entry["Item Loan Id"]
                else:
                    temp_fine["almaLoanId"] = r"\N"
                temp_fine["origAmtAlma"] = float(fine_entry["Original Amount"])
                temp_fine["remAmtAlma"] = float(fine_entry["Remaining Amount"])
                note = fine_entry["Fine Comment"]
                sierra_note_parsed = parseSierraNote(note)
                if sierra_note_parsed is None:
                    temp_fine["isSierraFine"] = False
                else:
                    temp_fine["isSierraFine"] = True

                if (
                    sierra_note_parsed is not None
                    and "invoice" not in sierra_note_parsed
                ):
                    msg = f"Found Sierra Fine with no invoice on fine {f['rootAlmaFineId']}"
                    errorCaught("Table To Fines", msg)

                elif sierra_note_parsed is not None and "invoice" in sierra_note_parsed:
                    # persist sierra_fine_details
                    temp_fine["sierra_fine_detail"] = sierra_note_parsed

                    # popluate fine note from imported note, if available
                    if (
                        "fineComment" in sierra_note_parsed
                        and sierra_note_parsed["fineComment"] is not None
                    ):
                        temp_fine["almaNote"] = sierra_note_parsed["fineComment"]
                    else:
                        temp_fine["almaNote"] = None

                    # associate initial Bursar metadata
                    temp_fine = sfLookup.addBursMdsToSierraFine(
                        finesLookupDict, temp_fine, problemFines
                    )
                    # associate initial TransBurs
                    temp_fine = sfLookup.addTransBursToSierraFine(temp_fine)

                    # populate item details from Sierra note
                    item_detail = {}

                    if "barcode" in sierra_note_parsed:
                        item_detail["barcode"] = sierra_note_parsed["barcode"]
                    else:
                        item_detail["barcode"] = None

                    if "itemTitle" in sierra_note_parsed:
                        item_detail["title"] = sierra_note_parsed["itemTitle"]
                    else:
                        item_detail["title"] = None

                    if "outDate" in sierra_note_parsed:
                        item_detail["loanDate"] = sierra_note_parsed["outDate"]
                    else:
                        item_detail["loanDate"] = None

                    if "dueDate" in sierra_note_parsed:
                        item_detail["dueDate"] = sierra_note_parsed["dueDate"]
                    else:
                        item_detail["dueDate"] = None

                    if "returnedDate" in sierra_note_parsed:
                        item_detail["returnedDate"] = sierra_note_parsed["returnedDate"]
                    else:
                        item_detail["returnedDate"] = None

                    temp_fine["item_detail"].append(item_detail)

                else:
                    # Typical fine with no Sierra Data
                    item_detail = {}
                    item_detail["locationCode"] = fine_entry[
                        "law-stks ELSE Location Code"
                    ]
                    temp_fine["almaNote"] = note
                    item_detail["author"] = fine_entry["Author"]
                    item_detail["callNumber"] = fine_entry["Call Number"]
                    item_detail["title"] = fine_entry["Loan Details"]

                    if fine_entry["Barcode"] != "-1":
                        item_detail["barcode"] = fine_entry["Barcode"]
                    else:
                        item_detail["barcode"] = None

                    if "Loan Date" in fine_entry and "Loan Time" in fine_entry:
                        if isinstance(fine_entry["Loan Date"], str) and isinstance(
                            fine_entry["Loan Time"], str
                        ):
                            loanDate = (
                                fine_entry["Loan Date"] + "T" + fine_entry["Loan Time"]
                            )
                            item_detail["loanDate"] = datetime.strptime(
                                loanDate, item_date_format
                            )

                    if "Due Date" in fine_entry and "Due Time" in fine_entry:
                        if isinstance(fine_entry["Due Date"], str) and isinstance(
                            fine_entry["Due Time"], str
                        ):
                            loanDate = (
                                fine_entry["Due Date"] + "T" + fine_entry["Due Time"]
                            )
                            item_detail["dueDate"] = datetime.strptime(
                                loanDate, item_date_format
                            )

                    if "Return Date" in fine_entry and "Return Time" in fine_entry:
                        if isinstance(fine_entry["Return Date"], str) and isinstance(
                            fine_entry["Return Time"], str
                        ):
                            returnedDate = (
                                fine_entry["Return Date"]
                                + "T"
                                + fine_entry["Return Time"]
                            )
                            item_detail["returnedDate"] = datetime.strptime(
                                returnedDate, item_date_format
                            )
                        else:
                            item_detail["returnedDate"] = None

                    temp_fine["item_detail"].append(item_detail)

                    if temp_fine["type"] != "Credit":
                        # Create an initial BursFineMd. Remainder of md fields
                        # will be calculated when initial fine is exported to
                        # Bursar

                        bursFineMd = {}

                        bursFineMd["bursFineId"] = temp_fine["rootAlmaFineId"]

                        if temp_fine["type"] == "Lost item replacement fee":
                            bursFineMd["isReplFee"] = True
                        else:
                            bursFineMd["isReplFee"] = False

                        # if item_detail["locationCode"] == "law-stks":
                        # bursFineMd["chargeAcc"] = "531000000005"
                        # elif bursFineMd["isReplFee"] == True:
                        # bursFineMd["chargeAcc"] = "531000000001"
                        # else:
                        # bursFineMd["chargeAcc"] = "531000000002"

                        searchObj = re.search(
                            r"(^law)|(_lw_)", item_detail["locationCode"]
                        )
                        if searchObj:
                            bursFineMd["chargeAcc"] = "531000000005"
                        else:
                            if bursFineMd["isReplFee"] == True:
                                bursFineMd["chargeAcc"] = "531000000001"
                            else:
                                bursFineMd["chargeAcc"] = "531000000002"

                        temp_fine["burs_fine_md"] = []
                        temp_fine["burs_fine_md"].append(bursFineMd)
                        # log.info(f"{bursFineMd}")  ###

                fines.append(temp_fine)

        if len(problemFines) > 0:
            pfids = [problemFine["rootAlmaFineId"] for problemFine in problemFines]
            msg = f"Found {len(problemFines)} problem fines: {', '.join(pfids)}"
            errorCaught("Table To Fines", msg)

        return fines


def tableToPatrons(patron_table):
    patrons_dict = patron_table.to_dict(orient="records")
    return patrons_dict


def updatePatrons(almaPatronsDict):
    log.info(f"number patrons from alma: {len(almaPatronsDict)}")

    # Faster version of comparing the Alma analytics report to our DB: use
    # Pandas to convert to dataframes and filter rows that are different that
    # came from the ALma analytics report

    # get all patron entries from DB, drop extraneous columns & rename to match
    # columns from Alma analytics report
    patrons_from_db = mongoPortal.getAll("patrons")
    df_from_psql = pd.DataFrame(patrons_from_db)
    df_from_psql.drop(columns=["created", "updated", "id", "_id"], inplace=True)
    df_from_psql.rename(
        columns={"almaPatronId": "Primary Identifier", "balance": "Remaining Amount"},
        inplace=True,
    )

    # make sure "Remaining Amounts" from Alma are floats
    df_from_alma = pd.DataFrame(almaPatronsDict)
    df_from_alma["Remaining Amount"] = df_from_alma["Remaining Amount"].astype(
        "float64"
    )

    # create a comparison dataframe for the two
    comparison_df = df_from_psql.merge(df_from_alma, indicator=True, how="outer")

    # "right only" rows are the implied change or new rows
    diff_df = comparison_df[comparison_df["_merge"] == "right_only"]

    # both means rows that are the same in both dataframes
    unchanged_df = comparison_df[comparison_df["_merge"] == "both"]

    numNewPats = 0
    numUpdatedPats = 0
    numUnchangedPats = len(unchanged_df)

    if len(diff_df) > 0:
        change_ids = diff_df["Primary Identifier"].values.tolist()
        log.info(
            f"Pandas calculations detected {len(change_ids)} patron rows that are different or do not exist in DB..."
        )

        log.info("Merging patrons to datastore:")

        for patron_id in tqdm(change_ids):
            p = mongoPortal.findOne("patrons", "almaPatronId", patron_id)
            pp = (
                list(
                    filter(
                        lambda patron: patron["Primary Identifier"] == patron_id,
                        almaPatronsDict,
                    )
                )
            )[0]
            if p is not None:
                if p["balance"] != float(pp["Remaining Amount"]):
                    numUpdatedPats += 1
                    mongoPortal.updatePatronRow(p, pp)
                else:
                    numUnchangedPats += 1
            else:
                numNewPats += 1
                mongoPortal.insertPatronRow(pp)

    if numNewPats == 0 and numUpdatedPats == 0 and numUnchangedPats == 0:
        return False
    else:
        log.info(
            f"patrons updated: {numUpdatedPats}; new patrons: {numNewPats}; unchanged patrons: {numUnchangedPats}"
        )
        return True


def updateFines(almaFinesDict, skipSierra):
    log.info(f"number fines from Alma: {len(almaFinesDict)}")

    patrons_from_db = mongoPortal.getAll("patrons")

    # Faster version of comparing the Alma analytics report to our DB: use
    # Pandas to convert to dataframes and filter rows that are different that
    # came from the ALma analytics report

    # get all fine entries from DB, drop extraneous columns
    fines_from_db = mongoPortal.getAll("fines")
    temp_df_a = pd.DataFrame(fines_from_db)
    fines_from_db_df = temp_df_a[
        ["rootAlmaFineId", "almaUpdated", "isClosedInAlma", "remAmtAlma", "almaNote"]
    ]

    # same thing for the updated fines data coming from Alma
    temp_df_b = pd.DataFrame(almaFinesDict)
    almaFines_df = temp_df_b[
        ["rootAlmaFineId", "almaUpdated", "isClosedInAlma", "remAmtAlma", "almaNote"]
    ]

    # create a comparison dataframe for the two
    comparison_df = fines_from_db_df.merge(almaFines_df, indicator=True, how="outer")
    # "right only" rows are the implied change or new rows
    diff_df = comparison_df[comparison_df["_merge"] == "right_only"]
    # both means rows that are the same in both dataframes
    unchanged_df = comparison_df[comparison_df["_merge"] == "both"]

    numNewFines = 0
    numUpdatedFines = 0
    numUnchangedFines = len(unchanged_df)

    if len(diff_df) > 0:
        change_ids = diff_df["rootAlmaFineId"].values.tolist()
        log.info(
            f"Pandas calculations detected {len(change_ids)} fines that are different or do not exist in DB..."
        )
        log.info("Merging fines to datastore:")
        for fine_id in tqdm(change_ids):
            f = mongoPortal.findOne("fines", "rootAlmaFineId", fine_id)
            ff = (
                list(
                    filter(
                        lambda fine: fine["rootAlmaFineId"] == fine_id, almaFinesDict
                    )
                )
            )[0]
            if f is not None:
                if f["isSierraFine"] == True and skipSierra == True:
                    pass
                else:
                    if (
                        (f["almaUpdated"] != ff["almaUpdated"])
                        or (f["isClosedInAlma"] != ff["isClosedInAlma"])
                        or (f["remAmtAlma"] != float(ff["remAmtAlma"]))
                        or (f["almaNote"] != ff["almaNote"])
                    ):
                        f["almaUpdated"] = ff["almaUpdated"]
                        f["isClosedInAlma"] = ff["isClosedInAlma"]
                        f["remAmtAlma"] = ff["remAmtAlma"]
                        f["almaNote"] = ff["almaNote"]
                        if len(ff["item_detail"]) > 0:
                            f["item_detail"] = ff["item_detail"]
                        numUpdatedFines += 1
                        log.debug(f"saving updated fine: {f['rootAlmaFineId']}")
                        ###update fine and add item(s)
                        mongoPortal.updateFineRow(f)
                        if len(f["item_detail"]) > 0:
                            for i in f["item_detail"]:
                                mongoPortal.insertItemDetail(i, f["id"])
                    else:
                        numUnchangedFines += 1
            else:
                # p = psqlConnect.findOne("patron", "almaPatronId", ff["almaPatronId"])
                p = (
                    list(
                        filter(
                            lambda patron: patron["almaPatronId"] == ff["almaPatronId"],
                            patrons_from_db,
                        )
                    )
                )[0]
                ff["patron"] = p["id"]
                # Add a root Alma Transaction
                ta = {}
                ta["almaFineId"] = ff["rootAlmaFineId"]
                # ta["almaTransId"] = Null
                ta["almaCreated"] = ff["almaCreated"]
                ta["amt"] = ff["origAmtAlma"]
                if moneyMath.isGtEqual(ff["origAmtAlma"], 0):
                    ta["type"] = "Fine"
                else:
                    ta["type"] = "Credit"
                if ff["isSierraFine"] == False and ta["type"] != "Credit":
                    ta["newAlmaImp"] = True
                else:
                    ta["newAlmaImp"] = False
                ff["trans_alma"] = []
                ff["trans_alma"].append(ta)
                numNewFines += 1
                log.debug(f"saving new fine: {ff['rootAlmaFineId']}")
                ###add fine, item(s) and alma_trans AND BURSAR MD
                mongoPortal.insertFineItemTransBurMD(ff)

    if numNewFines == 0 and numUpdatedFines == 0 and numUnchangedFines == 0:
        return False
    else:
        log.info(
            f"fines updated: {numUpdatedFines}; new fines: {numNewFines}; unchanged fines: {numUnchangedFines}"
        )
        return True


def updateCreditFines(almaCreditFinesDict):
    log.info(f"number credit fines from Alma: {len(almaCreditFinesDict)}")

    newCreditFineIds = []
    existingCreditFineIds = []
    newRootCreditFineIds = []
    existingRootCreditFineIds = []
    noMatchingLoanIds = []
    noMatchingPatronIds = []
    creditExceedsOwingFineIds = []
    splitCreditFineIds = []
    skippedCreditFineIds = []

    fines_from_db = mongoPortal.getAll("fines")
    trans_from_db = mongoPortal.getAll("trans_alma")

    for cf in tqdm(almaCreditFinesDict):
        cfId = cf["rootAlmaFineId"]
        f = list(filter(lambda fine: fine["rootAlmaFineId"] == cfId, fines_from_db))
        ta = list(filter(lambda trans: trans["almaFineId"] == cfId, trans_from_db))
        if len(f) != 0:
            existingRootCreditFineIds.append(cfId)
        elif len(ta) != 0:
            existingCreditFineIds.append(cfId)
        elif cf["almaLoanId"] is None:
            newRootCreditFineIds.append(cfId)
            ###add cf to Fines table
            mongoPortal.insertFineItem(cf)
        else:
            fines = mongoPortal.select_fines_Atrans_on_almaLoanId(cf["almaLoanId"])
            log.debug(f"fines with almaLoanId: {len(fines)}")
            if len(fines) == 0:
                noMatchingLoanIds.append(cfId)
                msg = f"Error: new Credit fine {cfId} with no matching loan id {cf['almaLoanId']}"
                errorCaught("Update Credit Fines", msg)
                ### log.error(msg)
                ### raise ValueError(msg)
                mongoPortal.insertFineItem(cf)
            else:
                if fines[0]["almaPatronId"] != cf["almaPatronId"]:
                    msg = f"Loan ids match, but patron ids do not match for new credit fine {cfId}"
                    noMatchingPatronIds.append(cfId)
                    errorCaught("Update Credit Fines", msg)
                    ### log.error(msg)
                    ### raise ValueError(msg)

                newCreditFineIds.append(cfId)
                credit = abs(cf["origAmtAlma"])
                transLists = [f["trans_alma"] for f in fines]
                cred_dict = moneyMath.distributeCreditAlma(credit, transLists)
                log.debug(
                    f"distributed credit: {cred_dict['creds']}, new amounts: {cred_dict['newAmts']}"
                )

                if moneyMath.isGt(cred_dict["remCred"], 0):
                    msg = f"original credit amount of {credit} exceeds the total amount of credit that could be applied to fines with shared loan id {cf['almaLoanId']} by {cred_dict['remCred']}."
                    creditExceedsOwingFineIds.append(cfId)
                    errorCaught("Update Credit Fines", msg)
                    ### log.error(msg)
                    ### raise ValueError(msg)

                creds_mapped = []
                for cred in cred_dict["creds"]:
                    if cred > 0:
                        creds_mapped.append(1)
                    else:
                        creds_mapped.append(0)
                numNonZeroCreds = sum(creds_mapped, 0)

                if numNonZeroCreds > 1:
                    log.info(
                        f"Applied credit accross two fines for loan Id {cf['almaLoanId']}"
                    )
                    splitCreditFineIds.append(cfId)

                for f in fines:
                    index = fines.index(f)
                    if moneyMath.isEqual(cred_dict["creds"][index], 0):
                        pass
                    else:
                        ta = {}
                        ta["amt"] = -(cred_dict["creds"][index])
                        ta["type"] = "Credit"
                        ta["almaFineId"] = cfId
                        ta["newAlmaImp"] = True
                        f["trans_alma"].append(ta)
                        ###add ta to DB if not there
                        for alma_trans in f["trans_alma"]:
                            if "id" not in alma_trans:
                                mongoPortal.insertAlmaTrans(alma_trans, f["id"])

    if (
        len(newCreditFineIds) == 0
        and len(existingCreditFineIds) == 0
        and len(newRootCreditFineIds) == 0
        and len(existingRootCreditFineIds) == 0
        and len(splitCreditFineIds) == 0
        and len(creditExceedsOwingFineIds) == 0
        and (len(noMatchingLoanIds) + len(noMatchingPatronIds)) == 0
        and len(skippedCreditFineIds) == 0
    ):
        return False
    else:
        r = {
            "newCreditTrans": len(newCreditFineIds),
            "existingCreditTrans": len(existingCreditFineIds),
            "newRootCredit": len(newRootCreditFineIds),
            "existingRootCredit": len(existingRootCreditFineIds),
            "splitCredit": len(splitCreditFineIds),
            "creditExceedsOwing": len(creditExceedsOwingFineIds),
            "noMatchingLoanIds": len(noMatchingLoanIds),
            "noMatchingPatronIds": len(noMatchingPatronIds),
        }
        log.info(f"updateCreditFines: {r}")
        return True


def updateTrans(almaTransDict, skipSierra):
    log.info(f"number transactions from Alma: {len(almaTransDict)}")

    # Faster version of comparing the Alma analytics report to our DB: use
    # Pandas to convert to dataframes and filter rows that are different that
    # came from the Alma analytics report
    temp_df_a = pd.DataFrame(almaTransDict)
    almaTrans_df = temp_df_a[["almaFineId", "almaNote", "almaTransId"]]

    # get all patron entries from DB, drop extraneous columns to match
    # columns from Alma analytics report
    transactions_from_db = mongoPortal.getAll("trans_alma")
    temp_df_b = pd.DataFrame(transactions_from_db)
    transactions_from_db_df = temp_df_b[["almaFineId", "almaNote", "almaTransId"]]

    # create a comparison dataframe for the two
    comparison_df = transactions_from_db_df.merge(
        almaTrans_df, indicator=True, how="outer"
    )
    # "right only" rows are the implied changed or new rows
    diff_df = comparison_df[comparison_df["_merge"] == "right_only"]
    # both means rows that are the same in both dataframes
    unchanged_df = comparison_df[comparison_df["_merge"] == "both"]

    numNewTrans = 0
    numUpdatedTrans = 0
    numUnchangedTrans = len(unchanged_df)

    if len(diff_df) > 0:
        change_ids = diff_df["almaTransId"].values.tolist()
        log.info("Merging fines to datastore:")
        for change_id in tqdm(change_ids):
            tt = list(
                filter(lambda trans: trans["almaTransId"] == change_id, almaTransDict)
            )[0]
            t = mongoPortal.findOne("trans_alma", "almaTransId", change_id)
            if t is not None:
                if (t["almaNote"] != tt["almaNote"]) or (
                    t["almaFineId"] != tt["almaFineId"]
                ):
                    t["almaNote"] = tt["almaNote"]
                    t["almaFineId"] = tt["almaFineId"]
                    numUpdatedTrans += 1
                    ### update on trans_alma table
                    mongoPortal.updateAlmaTransRow(t)
                else:
                    numUnchangedTrans += 1
            else:
                f = mongoPortal.findOne("fines", "rootAlmaFineId", tt["almaFineId"])
                if f is not None:
                    if f["isSierraFine"] == True and skipSierra == True:
                        pass
                    else:
                        fine_id = f["id"]
                        tt["newAlmaImp"] = True
                        numNewTrans += 1
                        ### add transaction
                        mongoPortal.insertAlmaTrans(tt, fine_id)

    if numNewTrans == 0 and numUpdatedTrans == 0 and numUnchangedTrans == 0:
        return False
    else:
        r = {
            "updated": numUpdatedTrans,
            "new": numNewTrans,
            "unchanged": numUnchangedTrans,
        }
        log.info(f"importAlmaService.updateTrans: {r}")
        return True


def applyNewPayments():
    """Updates TransBurs with TransAlma new Payment transactions, then
    returns the number of updated payments.
    """
    # find all new Payments
    newPayTa = mongoPortal.findAllNewPayments()
    if newPayTa is None:
        log.info(
            f"importAlmaService.applyNewPayments: No new new payment transactions to process"
        )
        return True
    else:
        payStats = {
            "new": 0,
            "splitAcrossBursAccs": 0,
            "skippedOverpayments": 0,
            "unprocessedTransCanceledOutPayment": 0,
            "otherError": 0,
        }
        newTb = []
        for ta in tqdm(newPayTa):
            # log.info(f"processing TransAlma with id: {ta['id']}")
            f = mongoPortal.select_f_ta_tb_bMD_on_almaFineId(ta["almaFineId"])
            if f is None:
                msg = f"Error: no matching fine found for Alma transaction {ta['id']}"
                payStats["otherError"] += 1
                errorCaught("Apply New Payments", msg)
                ### log.error(msg)
                return False
            else:
                # log.info(f"found parent fine for TransAlma {ta['id']}: {f['rootAlmaFineId']}")
                remTransferAmt = abs(ta["amt"])
                fineNeedsUpdate = False
                for md in f["burs_fine_md"]:
                    temp_list = [
                        t["amt"]
                        for t in f["trans_burs"]
                        if t["bursFineId"] == md["bursFineId"]
                    ]
                    owe = moneyMath.correct(sum(temp_list))
                    # We only apply payment amounts to fine with positive balance.
                    if moneyMath.isLtEqual(owe, 0) or moneyMath.isEqual(
                        remTransferAmt, 0
                    ):
                        pass
                    else:
                        newOwe = max(0, owe - remTransferAmt)
                        # amount of payment applied to this fine
                        applied = moneyMath.correct(owe - newOwe)
                        # amount of payment left to be applied to other fine part
                        remTransferAmt = moneyMath.correct(remTransferAmt - applied)
                        tb = {}
                        tb["amt"] = -(applied)
                        tb["bursFineId"] = md["bursFineId"]
                        tb["newBursImp"] = False
                        tb["newBursExp"] = False
                        tb["type"] = "Payment"
                        # Add new Bursar Transaction
                        if "trans_burs" not in f:
                            f["trans_burs"] = []
                        f["trans_burs"].append(tb)
                        fineNeedsUpdate = True

                # NOTE: There is a potential bug in the logic here. On November 22,
                # 2019, this resulted in a $70 reversal being sent to the bursar in
                # error. No harm was done since the bursar refused the reversal, but
                # it would be better not to send something like this over when we know
                # it is a problem. Run the following to see this example:
                # lookupFine --rootAlmaFineId "12894251030003841"

                # NOTE 2: There was an action replay of this bug on January 11, 2021,
                # where a patron had 2 lost item replacement fees -- an Apple power
                # adapter ($90) and an Apple phone charger ($41) -- and payment for
                # the latter seemed to have been incorrectly applied to the former,
                # raising a situation where the patron has ($90 - $41) + $41 = $90 on
                # the account, instead of $90 + ($41 - $41) = $90. Note that this is
                # not necessarily a bug that can be accounted for, as it was seemingly
                # caused by human error.

                if moneyMath.isGt(remTransferAmt, 0):
                    payStats["skippedOverpayments"] += 1
                    msg = f"Payment from Alma transaction id {ta} would exceed amount owing by {remTransferAmt}."
                    errorCaught("Apply New Payments", msg)
                    ### log.error(msg)
                    ### return False

                if fineNeedsUpdate:
                    ta["newAlmaImp"] = False
                    ### await taRep.persist(ta)
                    now = mongoPortal.right_now()
                    trans_alma = mongoPortal.arise_mongo("trans_alma")
                    query = {"id": ta["id"]}
                    new_values = {"$set": {"newAlmaImp": False, "updated": now}}
                    trans_alma.update_one(query, new_values)
                    ### TransBurs is committed via cascade insert
                    for burs_trans in f["trans_burs"]:
                        if "id" not in burs_trans:
                            mongoPortal.insertBursTrans(burs_trans, f["id"])

                    payStats["new"] += 1

        if (
            payStats["new"] == 0
            and payStats["splitAcrossBursAccs"] == 0
            and payStats["skippedOverpayments"] == 0
            and payStats["unprocessedTransCanceledOutPayment"] == 0
            and payStats["otherError"] == 0
        ):
            return False
        else:
            log.info(f"importAlmaService.applyNewPayments: {payStats}")
            return True


def getLatestAlmaData():
    """Important to get data in order of transactions, fines then patrons to
    ensure that all transactions will have a fine and all fines will
    have a patron.
    """
    log.info("Getting latest Alma data")
    geDate = "2017-01-01T00:00:00"

    transTbl = almaAPI.getAllTransactions(geDate)
    almaTrans = tableToTrans(transTbl)
    log.info(f"Received {len(almaTrans)} transactions")

    finesTbl = almaAPI.getAllFines(geDate)
    tempFines = tableToFines(finesTbl)
    log.info(f"Received {len(tempFines)} fines")
    almaFines = []
    almaCreditFines = []
    for fine in tempFines:
        if fine["type"] == "Credit":
            almaCreditFines.append(fine)
        else:
            almaFines.append(fine)
    log.info(f"Parsed {len(almaFines)} debit fines")
    log.info(f"Parsed {len(almaCreditFines)} credit fines")
    sierraFines = [fine for fine in almaFines if fine["isSierraFine"] == True]
    patronsTbl = almaAPI.getAllUserBalances()
    almaPatrons = tableToPatrons(patronsTbl)
    log.info(f"Received {len(almaPatrons)} patrons")

    if (
        len(almaPatrons) > 0
        and len(almaFines) > 0
        and len(almaCreditFines) > 0
        and len(almaTrans) > 0
        and len(sierraFines) > 0
    ):
        results = True
        return results, almaPatrons, almaFines, almaCreditFines, almaTrans, sierraFines
    else:
        results = False
        return results, almaPatrons, almaFines, almaCreditFines, almaTrans, sierraFines


def updateAll(almaPatronsDict, almaFinesDict, almaCreditFinesDict, almaTransDict):
    patronResult = updatePatrons(almaPatronsDict)
    if patronResult:
        fineResult = updateFines(almaFinesDict, False)
        if fineResult:
            creditFineResult = updateCreditFines(almaCreditFinesDict)
            if creditFineResult:
                transResult = updateTrans(almaTransDict, False)
                if transResult:
                    newPaymentResult = applyNewPayments()
                    if newPaymentResult:
                        return True, ""
                    else:
                        return False, "applyNewPayments()"
                else:
                    return False, "updateTrans()"
            else:
                return False, "updateCreditFines()"
        else:
            return False, "updateFines()"
    else:
        return False, "updatePatrons()"


def importAlmaTransactions():
    log.info("Alma Analytics acquisition & merge to local datastore started...")
    job_id = jobUtils.startNewJob("AlmaImport")
    (
        resultsD,
        almaPatronsD,
        almaFinesD,
        almaCreditFinesD,
        almaTransD,
        sierraFinesD,
    ) = getLatestAlmaData()
    if not resultsD:
        msg = "Failed to import new data from Alma; latest retrieved Patrons, Fines, Transactions, and Sierra Fines data were all empty, so obviously something is wrong."
        log.error(msg)
        jobUtils.jobFail(job_id, msg)
        slacker("Alma Analytics Import", msg)
        sys.exit(-1)
    else:
        # After getLatestAlmaData returns true, updateAll should be called to
        # send new and updated data to internal database.
        update_results, update_msg = updateAll(
            almaPatronsD, almaFinesD, almaCreditFinesD, almaTransD
        )
        if update_results:
            jobUtils.jobWin(job_id)
            msg = "Successfully imported new data from Alma and merged new data to database."
            log.info(msg)
            slacker("Alma Analytics Import", msg)
            return True
        else:
            msg = f"Failed to merge new data to datastore; the process failed on {update_msg}. Perhaps check the logs."
            log.error(msg)
            slacker("Alma Analytics Import", msg)
            jobUtils.jobFail(job_id, msg)
            sys.exit(-1)
