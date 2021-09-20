import json
import re
from datetime import datetime
import os

# local modules
import moneyMath
from logger import log
from auth_puller import auth_puller


def getMd(mds, isRepl):
    if mds == "" or len(mds) == 0 or mds is None:
        return None
    else:
        returned_mds = [md for md in mds if md["isReplFee"] == isRepl]
        if len(returned_mds) > 0:
            return returned_mds
        else:
            return None


def getTempArray():
    """Loads the sierra_fine_xref.json file.
    """
    paths = auth_puller("auth.json", "paths")
    sierra_fine_xref_file = os.path.join(
        paths["local_path"], paths["db_data"], "sierra_fine_xref.json"
    )
    with open(sierra_fine_xref_file, "r") as f:
        tempArray = json.load(f)
    return tempArray


def getFinesLookup():
    # SierraBursarDataPair = { replFeePart: SierraBursarData, billFeePart: SierraBursarData}
    tempArrayDateForm = "%Y-%m-%dT%H:%M:%S"
    ta = getTempArray()
    _finesLookup = {}
    for ta_fine in ta:
        bursar_ref_id_test = re.search(r"_0[13]$", ta_fine["bursar_ref_id"])
        if bursar_ref_id_test:
            sbd = {}
            bursar_ref_id_repl_test = re.search(r"_01$", ta_fine["bursar_ref_id"])
            fbYear = int("20" + ta_fine["item_term"][1:3])
            if "sf_invoice_date" in ta_fine and ta_fine["sf_invoice_date"] is not None:
                bbDate = datetime.strptime(
                    (ta_fine["sf_invoice_date"])[:-5], tempArrayDateForm
                )
            else:
                bbDate = datetime(fbYear, 1, 1)
            if "sf_due_date" in ta_fine and ta_fine["sf_due_date"] is not None:
                bdDate = datetime.strptime(
                    (ta_fine["sf_due_date"])[:-5], tempArrayDateForm
                )
            else:
                bdDate = datetime(fbYear, 2, 25)
            sbd["bursarFineId"] = ta_fine["bursar_ref_id"]
            sbd["rootAlmaFineId"] = ta_fine["alma_fine_id"]
            sbd["almaPatronId"] = ta_fine["emplid"]
            sbd["account"] = ta_fine["account"]
            sbd["termCode"] = ta_fine["item_term"]
            sbd["bursarBillingDate"] = bbDate
            sbd["bursarDueDate"] = bdDate
            if bursar_ref_id_repl_test:
                sbd["amt"] = ta_fine["item_fee_amt"]
                if ta_fine["sierra_invoice_id"] in _finesLookup:
                    _finesLookup[(ta_fine["sierra_invoice_id"])]["replFeePart"] = sbd
                else:
                    _finesLookup[(ta_fine["sierra_invoice_id"])] = {}
                    _finesLookup[(ta_fine["sierra_invoice_id"])]["replFeePart"] = sbd
            else:
                sbd["amt"] = ta_fine["billing_fee_amt"]
                if ta_fine["sierra_invoice_id"] in _finesLookup:
                    _finesLookup[(ta_fine["sierra_invoice_id"])]["billFeePart"] = sbd
                else:
                    _finesLookup[(ta_fine["sierra_invoice_id"])] = {}
                    _finesLookup[(ta_fine["sierra_invoice_id"])]["billFeePart"] = sbd
        else:
            pass

    for key, value in _finesLookup.items():
        if "replFeePart" not in value:
            _finesLookup[key]["replFeePart"] = None
        elif "billFeePart" not in value:
            _finesLookup[key]["billFeePart"] = None
        else:
            pass

    return _finesLookup


def getOrigSierraFine(lookup, sierra_invoice_id, part):
    if sierra_invoice_id in lookup:
        pair = lookup[sierra_invoice_id]
        return pair[part]
    else:
        return None


def addBursMdsToSierraFine(lookup_dict, fine, problem_Fines_list):
    """All Sierra fines were already sent to PS and therefore have a billing
    date, fine due date and all other metadata associated with them.
    """
    sierra_details = fine["sierra_fine_detail"]
    if moneyMath.isGt(sierra_details["itemFee"], 0):
        sbd = getOrigSierraFine(lookup_dict, sierra_details["invoice"], "replFeePart")
        if sbd is None:
            msg = f"Could not find bursar data for patron {fine['almaPatronId']} with invoice {sierra_details['invoice']} and with item replacement fee {sierra_details['itemFee']}"
            log.warning(msg)
            problem_Fines_list.append(fine)
        else:
            b = {}
            b["isReplFee"] = True
            b["bursFineId"] = sbd["bursarFineId"]
            if b["bursFineId"] == "":
                log.error(f"No bursar fine ID parsed for fine {fine} with sdb: {sdb}")
            b["billingDate"] = sbd["bursarBillingDate"]
            b["fineDueDate"] = sbd["bursarDueDate"]
            b["chargeAcc"] = sbd["account"]
            b["termCode"] = sbd["termCode"]
            if "burs_fine_md" in fine:
                fine["burs_fine_md"].append(b)
            else:
                fine["burs_fine_md"] = []
                fine["burs_fine_md"].append(b)
    if moneyMath.isGt(sierra_details["billingFee"], 0):
        sbd = getOrigSierraFine(lookup_dict, sierra_details["invoice"], "billFeePart")
        if sbd is None:
            msg = f"Could not find bursar data for patron {fine['almaPatronId']} with invoice {sierra_details['invoice']} and with billing (fine) fee {sierra_details['billingFee']}"
            log.warning(msg)
            problem_Fines_list.append(fine)
        else:
            b = {}
            b["isReplFee"] = False
            b["bursFineId"] = sbd["bursarFineId"]
            if b["bursFineId"] == "":
                log.error(f"No bursar fine ID parsed for fine {fine} with sdb: {sdb}")
            b["billingDate"] = sbd["bursarBillingDate"]
            b["fineDueDate"] = sbd["bursarDueDate"]
            b["chargeAcc"] = sbd["account"]
            b["termCode"] = sbd["termCode"]
            if "burs_fine_md" in fine:
                fine["burs_fine_md"].append(b)
            else:
                fine["burs_fine_md"] = []
                fine["burs_fine_md"].append(b)
    if sierra_details["itemFee"] + sierra_details["billingFee"] != fine["origAmtAlma"]:
        msg = f"!!!!! item fee {sierra_details['itemFee']} + billing fee {sierra_details['billingFee']} != original Alma amount {fine['origAmtAlma']} for fine {fine['rootAlmaFineId']}"
        log.error(msg)
        raise ValueError(msg)
    return fine


def addTransBursToSierraFine(fine):
    sierra_fine_detail = fine["sierra_fine_detail"]
    mds = fine["burs_fine_md"]
    fine["trans_burs"] = []
    if sierra_fine_detail["itemFee"] == "":
        itemFee = 0
    else:
        itemFee = sierra_fine_detail["itemFee"]
    if sierra_fine_detail["billingFee"] == "":
        billFee = 0
    else:
        billFee = sierra_fine_detail["billingFee"]
    curAmt = fine["origAmtAlma"]
    if moneyMath.isGt(itemFee, 0):
        md = getMd(mds, True)
        if md:
            tb = {}
            tb["newBursImp"] = False
            tb["newBursExp"] = False
            tb["amt"] = min(curAmt, itemFee)
            # reduce amount to apply to billingFee by itemFee:
            curAmt = moneyMath.correct(curAmt - tb["amt"])
            tb["type"] = "Fine"
            tb["bursFineId"] = md[0]["bursFineId"]
            tb["bursCreated"] = md[0]["billingDate"]
            fine["trans_burs"].append(tb)
        else:
            pass
    if moneyMath.isGt(billFee, 0):
        md = getMd(mds, False)
        if md:
            tb = {}
            tb["newBursImp"] = False
            tb["newBursExp"] = False
            tb["amt"] = moneyMath.correct(min(curAmt, billFee))
            tb["type"] = "Fine"
            tb["bursFineId"] = md[0]["bursFineId"]
            tb["bursCreated"] = md[0]["billingDate"]
            fine["trans_burs"].append(tb)
        else:
            pass
    return fine
