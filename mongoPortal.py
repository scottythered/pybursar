import json
import sys
import pymongo
from bson.objectid import ObjectId
from auth_puller import auth_puller
import datetime
from datetime import datetime
from logger import log


def instantiate_almabursar_DB():
    mg_auth = auth_puller("auth.json", "mongo")
    client = pymongo.MongoClient(mg_auth["uri"])
    return client.almabursar


def arise_mongo(kind):
    almabursar = instantiate_almabursar_DB()
    if kind == "fine_mds":
        fine_mds = almabursar.fine_mds
        return fine_mds
    elif kind == "fines":
        fines = almabursar.fines
        return fines
    elif kind == "patrons":
        patrons = almabursar.patrons
        return patrons
    elif kind == "burs_exports":
        burs_exports = almabursar.burs_exports
        return burs_exports
    elif kind == "trans_burs":
        trans_burs = almabursar.trans_burs
        return trans_burs
    elif kind == "trans_alma":
        trans_alma = almabursar.trans_alma
        return trans_alma
    elif kind == "item_details":
        item_details = almabursar.item_details
        return item_details
    elif kind == "burs_export_trans":
        burs_export_trans = almabursar.burs_export_trans
        return burs_export_trans
    elif kind == "jobs":
        jobs = almabursar.jobs
        return jobs
    elif kind == "burs_imports":
        burs_imports = almabursar.burs_imports
        return burs_imports
    else:
        raise ValueError("Unknown collection name queried.")


def right_now():
    now = datetime.utcnow()
    now_truncated = datetime(
        now.year, now.month, now.day, now.hour, now.minute, now.second
    )
    return now_truncated


def df_check(dataframe, dictionary, list_name):
    if len(dataframe) > 0:
        raw_json = dataframe.to_json(orient="records", date_format="iso")
        parsed_json = json.loads(raw_json)
        dictionary[list_name] = [j for j in parsed_json]
    else:
        pass


def dfToDictList(dataframe):
    df_dict = dataframe.to_dict(orient="records")
    if len(df_dict) == 0:
        return None
    elif isinstance(df_dict, list):
        return df_dict
    elif isinstance(df_dict, dict):
        return [df_dict]


def id_deserialize(records_list):
    for rec in records_list:
        rec["_id"] = str(rec["_id"])
    return records_list


def getAll(db_name):
    collection = arise_mongo(db_name)
    collection_export = list(collection.find())
    return id_deserialize(collection_export)


def findOne(db, field, search_value):
    mg_auth = auth_puller("auth.json", "mongo")
    client = pymongo.MongoClient(mg_auth["uri"])
    almabursar = client.almabursar
    if db == "patrons":
        patrons = almabursar.patrons
        results = list(patrons.find({field: search_value}))
    elif db == "fine_mds":
        fine_mds = almabursar.fine_mds
        results = list(fine_mds.find({field: search_value}))
    elif db == "fines":
        fines = almabursar.fines
        results = list(fines.find({field: search_value}))
    elif db == "burs_exports":
        burs_exports = almabursar.burs_exports
        results = list(burs_exports.find({field: search_value}))
    elif db == "trans_alma":
        trans_alma = almabursar.trans_alma
        results = list(trans_alma.find({field: search_value}))
    elif db == "trans_burs":
        trans_burs = almabursar.trans_burs
        results = list(trans_burs.find({field: search_value}))
    elif db == "burs_export_trans":
        burs_export_trans = almabursar.burs_export_trans
        results = list(burs_export_trans.find({field: search_value}))
    elif db == "jobs":
        jobs = almabursar.jobs
        results = list(jobs.find({field: search_value}))
    if len(results) == 1:
        return results[0]
    elif len(results) > 1:
        raise ValueError(
            f"Hoped to find one result in {db} for {search_value} but got {len(results)} instead!"
        )
    else:
        return None


def findMany(db, field, search_value):
    mg_auth = auth_puller("auth.json", "mongo")
    client = pymongo.MongoClient(mg_auth["uri"])
    almabursar = client.almabursar
    if db == "item_details":
        item_details = almabursar.item_details
        results = list(item_details.find({field: search_value}))
    elif db == "patrons":
        patrons = almabursar.patrons
        results = list(patrons.find({field: search_value}))
    elif db == "fine_mds":
        fine_mds = almabursar.fine_mds
        results = list(fine_mds.find({field: search_value}))
    elif db == "fines":
        fines = almabursar.fines
        results = list(fines.find({field: search_value}))
    elif db == "burs_exports":
        burs_exports = almabursar.burs_exports
        results = list(burs_exports.find({field: search_value}))
    elif db == "trans_alma":
        trans_alma = almabursar.trans_alma
        results = list(trans_alma.find({field: search_value}))
    elif db == "trans_burs":
        trans_burs = almabursar.trans_burs
        results = list(trans_burs.find({field: search_value}))
    elif db == "burs_export_trans":
        burs_export_trans = almabursar.burs_export_trans
        results = list(burs_export_trans.find({field: search_value}))
    elif db == "jobs":
        jobs = almabursar.jobs
        results = list(jobs.find({field: search_value}))
    if len(results) > 0:
        return results
    else:
        return None


def updatePatronRow(original, newer):
    patrons = arise_mongo("patrons")
    now = right_now()
    query = {"almaPatronId": original["almaPatronId"]}
    new_values = {"$set": {"updated": now, "balance": float(newer["Remaining Amount"])}}
    result = patrons.update_one(query, new_values)
    if result.modified_count == 1 and result.matched_count == 1:
        return
    else:
        log.error(f"Updated more rows than anticipated on patron ID {patronID}")
        sys.exit(-1)


def insertPatronRow(new_patron):
    patrons = arise_mongo("patrons")
    now = right_now()
    last_id = (list(patrons.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "almaPatronId": new_patron["Primary Identifier"],
        "balance": float(new_patron["Remaining Amount"]),
        "created": now,
        "updated": now,
        "id": new_id,
    }
    result = patrons.insert_one(new)
    if result.acknowledged is True:
        return
    else:
        log.error(
            f"Expected to add new patron {new_patron['Primary Identifier']} but insertion was not acknowledged."
        )
        sys.exit(-1)


def updateFineRow(updated_fine):
    fines = arise_mongo("fines")
    now = right_now()
    query = {"rootAlmaFineId": updated_fine["rootAlmaFineId"]}
    new_values = {
        "$set": {
            "updated": now,
            "almaNote": updated_fine["almaNote"],
            "remAmtAlma": updated_fine["remAmtAlma"],
            "isClosedInAlma": updated_fine["isClosedInAlma"],
            "almaUpdated": updated_fine["almaUpdated"],
        }
    }
    result = fines.update_one(query, new_values)
    if result.modified_count == 1 and result.matched_count == 1:
        return
    else:
        log.error(
            f"Updated more rows than anticipated on fine {updated_fine['rootAlmaFineId']}"
        )
        sys.exit(-1)


def insertItemDetail(item_detail, fine_id):
    now = right_now()
    if "barcode" not in item_detail:
        item_detail["barcode"] = None
    if "author" not in item_detail:
        item_detail["author"] = None
    if "callNumber" not in item_detail:
        item_detail["callNumber"] = None
    if "title" not in item_detail:
        item_detail["title"] = None
    if "returnedDate" not in item_detail:
        item_detail["returnedDate"] = None
    if "loanDate" not in item_detail:
        item_detail["loanDate"] = None
    if "dueDate" not in item_detail:
        item_detail["dueDate"] = None
    if "locationCode" not in item_detail:
        item_detail["locationCode"] = None
    item_details = arise_mongo("item_details")
    last_id = (list(item_details.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "title": item_detail["title"],
        "author": item_detail["author"],
        "callNumber": item_detail["callNumber"],
        "loanDate": item_detail["loanDate"],
        "dueDate": item_detail["dueDate"],
        "returnedDate": item_detail["returnedDate"],
        "locationCode": item_detail["locationCode"],
        "created": now,
        "updated": now,
        "fine": fine_id,
        "barcode": item_detail["barcode"],
        "id": new_id,
    }
    result = item_details.insert_one(new)
    if result.acknowledged is True:
        return
    else:
        log.error(
            f"Expected to add new item for fine {fine_id} but insertion was not acknowledged."
        )
        sys.exit(-1)


def insertAlmaTrans(trans_detail, fine_id):
    now = right_now()
    if "almaCreated" not in trans_detail:
        trans_detail["almaCreated"] = None
    if "almaNote" not in trans_detail:
        trans_detail["almaNote"] = None
    if "almaTransId" not in trans_detail:
        trans_detail["almaTransId"] = None
    trans_alma = arise_mongo("trans_alma")
    last_id = (list(trans_alma.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "almaFineId": trans_detail["almaFineId"],
        "almaCreated": trans_detail["almaCreated"],
        "type": trans_detail["type"],
        "amt": trans_detail["amt"],
        "newAlmaImp": trans_detail["newAlmaImp"],
        "created": now,
        "updated": now,
        "fine": fine_id,
        "almaNote": trans_detail["almaNote"],
        "almaTransId": trans_detail["almaTransId"],
        "id": new_id,
    }
    result = trans_alma.insert_one(new)
    if result.acknowledged is True:
        return
    else:
        log.error(
            f"Expected to add new alma transaction for fine {fine_id} but insertion was not acknowledged."
        )
        sys.exit(-1)


def insertBursTrans(burs_detail, fine_id):
    now = right_now()
    trans_burs = arise_mongo("trans_burs")
    last_id = (list(trans_burs.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    if "bursTransId" not in burs_detail:
        burs_detail["bursTransId"] = None
    if "bursCreated" not in burs_detail:
        burs_detail["bursCreated"] = None
    new = {
        "bursFineId": burs_detail["bursFineId"],
        "bursTransId": burs_detail["bursTransId"],
        "bursCreated": burs_detail["bursCreated"],
        "type": burs_detail["type"],
        "amt": burs_detail["amt"],
        "newBursImp": burs_detail["newBursImp"],
        "newBursExp": burs_detail["newBursExp"],
        "created": now,
        "updated": now,
        "fine": fine_id,
        "id": new_id,
    }
    result = trans_burs.insert_one(new)
    if result.acknowledged is True:
        return
    else:
        log.error(
            f"Expected to add new bursar transaction for fine {fine_id} but insertion was not acknowledged."
        )
        sys.exit(-1)


def insertBursarMD(bursarMD_detail, fine_id):
    if "billingDate" not in bursarMD_detail:
        bursarMD_detail["billingDate"] = None
    if "fineDueDate" not in bursarMD_detail:
        bursarMD_detail["fineDueDate"] = None
    if "termCode" not in bursarMD_detail:
        bursarMD_detail["termCode"] = None
    fine_mds = arise_mongo("fine_mds")
    last_id = (list(fine_mds.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "bursFineId": bursarMD_detail["bursFineId"],
        "alma_fine_id": bursarMD_detail["bursFineId"],
        "isReplFee": bursarMD_detail["isReplFee"],
        "chargeAcc": bursarMD_detail["chargeAcc"],
        "fine": fine_id,
        "billingDate": bursarMD_detail["billingDate"],
        "fineDueDate": bursarMD_detail["fineDueDate"],
        "termCode": bursarMD_detail["termCode"],
        "id": new_id,
        "sierra_item_detail": None,
    }
    result = fine_mds.insert_one(new)
    if result.acknowledged is True:
        return
    else:
        log.error(
            f"Expected to add new bursar fine ID for fine {fine_id} but insertion was not acknowledged."
        )
        sys.exit(-1)


def insertFineItemTransBurMD(fine_package):
    now = right_now()
    fines = arise_mongo("fines")
    last_id = (list(fines.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "almaPatronId": fine_package["almaPatronId"],
        "rootAlmaFineId": fine_package["rootAlmaFineId"],
        "almaLoanId": fine_package["almaLoanId"],
        "isSierraFine": fine_package["isSierraFine"],
        "almaCreated": fine_package["almaCreated"],
        "almaUpdated": fine_package["almaUpdated"],
        "type": fine_package["type"],
        "origAmtAlma": fine_package["origAmtAlma"],
        "remAmtAlma": fine_package["remAmtAlma"],
        "isClosedInAlma": fine_package["isClosedInAlma"],
        "almaNote": fine_package["almaNote"],
        "created": now,
        "updated": now,
        "patron": fine_package["patron"],
        "id": new_id,
    }
    result = fines.insert_one(new)
    if result.acknowledged is True:
        for item_detail in fine_package["item_detail"]:
            insertItemDetail(item_detail, new_id)
        for trans_detail in fine_package["trans_alma"]:
            insertAlmaTrans(trans_detail, new_id)
        for burs_fine_md in fine_package["burs_fine_md"]:
            insertBursarMD(burs_fine_md, new_id)
    else:
        log.error(
            f"Expected to add new row(s) for fine {fine_package['rootAlmaFineId']} but insertion was not acknowledged."
        )
        sys.exit(-1)


def insertFineItem(fine_package):
    now = right_now()
    fines = arise_mongo("fines")
    last_id = (list(fines.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "almaPatronId": fine_package["almaPatronId"],
        "rootAlmaFineId": fine_package["rootAlmaFineId"],
        "almaLoanId": fine_package["almaLoanId"],
        "isSierraFine": fine_package["isSierraFine"],
        "almaCreated": fine_package["almaCreated"],
        "almaUpdated": fine_package["almaUpdated"],
        "type": fine_package["type"],
        "origAmtAlma": fine_package["origAmtAlma"],
        "remAmtAlma": fine_package["remAmtAlma"],
        "isClosedInAlma": fine_package["isClosedInAlma"],
        "almaNote": fine_package["almaNote"],
        "created": now,
        "updated": now,
        "patron": fine_package["patron"],
        "id": new_id,
    }
    result = fines.insert_one(new)
    if result.acknowledged is True:
        # if "item_detail" not in fine_package:
        # fine_package["item_detail"] = [{"title": None, "author": None, "callNumber": None, "barcode": None}]
        for item_detail in fine_package["item_detail"]:
            insertItemDetail(item_detail, new_id)
    else:
        log.error(
            f"Expected to add new row(s) for fine {fine_package['rootAlmaFineId']} but insertion was not acknowledged."
        )
        sys.exit(-1)


def select_fines_Atrans_on_almaLoanId(almaLoanId):
    found_fines = findMany("fines", "almaLoanId", almaLoanId)
    if found_fines is not None:
        fine_data_dicts = id_deserialize(found_fines)
        for fine in fine_data_dicts:
            fine_id = fine["id"]
            transAlma = findMany("trans_alma", "fine", fine_id)
            fine["trans_alma"] = transAlma
        return fine_data_dicts
    else:
        return []


def updateAlmaTransRow(almaTransRow):
    now = right_now()
    trans_alma = arise_mongo("trans_alma")
    new_values = {
        "$set": {
            "almaNote": almaTransRow["almaNote"],
            "almaFineId": almaTransRow["almaFineId"],
            "updated": now,
        }
    }
    query = {"id": almaTransRow["id"]}
    result = trans_alma.update_one(query, new_values)
    if result.modified_count == 1 and result.matched_count == 1:
        return
    else:
        log.error(
            f"Updated more rows than anticipated on alma transaction ID {almaTransRow['almaNote']}"
        )
        sys.exit(-1)


def findAllNewPayments():
    pmtTypStr = "Payment"
    trans_alma = arise_mongo("trans_alma")
    results = list(trans_alma.find({"newAlmaImp": True, "type": pmtTypStr}))
    if len(results) > 0:
        return id_deserialize(results)
    else:
        return None


def select_f_ta_tb_bMD_on_almaFineId(almaFineId):
    fine_match = findOne("fines", "rootAlmaFineId", almaFineId)
    if fine_match is None:
        return None
    else:
        join_tables = ["fine_mds", "trans_alma", "trans_burs"]
        fine_id = fine_match["id"]
        for table in join_tables:
            if table == "fine_mds":
                list_name = "burs_fine_md"
            else:
                list_name = table
            result = findMany(table, "fine", fine_id)
            if result is not None:
                fine_match[list_name] = result
            else:
                fine_match[list_name] = []
        return fine_match


def insertBursExportRow():
    now = right_now()
    burs_exports = arise_mongo("burs_exports")
    last_id = (list(burs_exports.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "chargesFilename": None,
        "reversalsFilename": None,
        "sent_to_bursar": None,
        "numSent": None,
        "created": now,
        "updated": now,
        "id": new_id,
    }
    result = burs_exports.insert_one(new)
    if result.acknowledged is True:
        return new_id
    else:
        log.error(
            f"Expected to add new BursExport Row {new_id} but insertion was not acknowledged."
        )
        sys.exit(-1)


def updateBursExportRow(chargesFilename, reversalsFilename, numSent, bursExportId):
    now = right_now()
    burs_exports = arise_mongo("burs_exports")
    new_values = {
        "$set": {
            "chargesFilename": chargesFilename,
            "reversalsFilename": reversalsFilename,
            "numSent": numSent,
            "updated": now,
        }
    }
    query = {"id": bursExportId}
    result = burs_exports.update_one(query, new_values)
    if result.modified_count == 1 and result.matched_count == 1:
        return
    else:
        log.error(
            f"Updated more rows than anticipated on BursExport Row {bursExportId}"
        )
        sys.exit(-1)


def getNewAlmaImps():
    found = findMany("trans_alma", "newAlmaImp", True)
    if found is not None:
        fine_ids = []
        for ta in found:
            if ta["fine"] not in fine_ids:
                fine_ids.append(ta["fine"])
        return fine_ids
    else:
        return None


def select_fines_ta_tb_bMD_on_FineId(fine_id):
    fine_match = findOne("fines", "id", fine_id)
    if fine_match is None:
        return None
    else:
        join_tables = ["fine_mds", "trans_alma", "trans_burs"]
        for table in join_tables:
            if table == "fine_mds":
                list_name = "burs_fine_md"
            else:
                list_name = table
            result = findMany(table, "fine", fine_id)
            if result is not None:
                fine_match[list_name] = result
            else:
                fine_match[list_name] = []
        return fine_match


def set_newAlmaImp_to_false(id_val):
    now = right_now()
    trans_alma = arise_mongo("trans_alma")
    new_values = {"$set": {"newAlmaImp": False, "updated": now}}
    query = {"id": id_val}
    result = trans_alma.update_one(query, new_values)
    if result.modified_count == 1 and result.matched_count == 1:
        return
    else:
        log.error(f"Updated more rows than anticipated on trans_alma's ID {id_val}")
        sys.exit(-1)


def insertBursTrans(burs_detail, fine_id):
    now = right_now()
    if "bursTransId" not in burs_detail:
        burs_detail["bursTransId"] = None
    if "bursCreated" not in burs_detail:
        burs_detail["bursCreated"] = None
    trans_burs = arise_mongo("trans_burs")
    last_id = (list(trans_burs.find().sort([("id", -1)]).limit(1)))[0]["id"]
    new_id = last_id + 1
    new = {
        "bursFineId": burs_detail["bursFineId"],
        "bursTransId": burs_detail["bursTransId"],
        "bursCreated": burs_detail["bursCreated"],
        "type": burs_detail["type"],
        "amt": burs_detail["amt"],
        "newBursImp": burs_detail["newBursImp"],
        "newBursExp": burs_detail["newBursExp"],
        "created": now,
        "updated": now,
        "fine": fine_id,
        "id": new_id,
    }
    result = trans_burs.insert_one(new)
    if result.acknowledged is True:
        return
    else:
        log.error(
            f"Expected to add new BursTrans for fine {fine_id} but insertion was not acknowledged."
        )
        sys.exit(-1)


def findOne_for_get_tb_fine_id_pairs(tbFine, tbBursFineId):
    mg_auth = auth_puller("auth.json", "mongo")
    client = pymongo.MongoClient(mg_auth["uri"])
    almabursar = client.almabursar
    fine_mds = almabursar.fine_mds
    results = list(fine_mds.find({"fine": tbFine}))
    if len(results) == 1:
        return results[0]
    elif len(results) > 1:
        new_results = list(fine_mds.find({"bursFineId": tbBursFineId}))
        if len(new_results) == 1:
            return new_results[0]
        else:
            raise ValueError(
                f"Hoped to find one result in fine_mds for {tbFine}/{tbBursFineId} but got {len(results)}/{len(new_results)} (respectively) instead!"
            )
    else:
        return None


def get_tb_fine_id_pairs():
    returnable = []
    # Get trans_burs_id , fine_id  pairs
    search = findMany("trans_burs", "newBursExp", True)
    if search is None:
        return []
    else:
        for tb in search:
            md = findOne_for_get_tb_fine_id_pairs(tb["fine"], tb["bursFineId"])
            returnable.append({"tbId": tb["id"], "mdId": md["id"]})
        return returnable


def insertBursExportTransRow(bet_package):
    now = right_now()
    burs_export_trans = arise_mongo("burs_export_trans")
    last_id = (list(burs_export_trans.find().sort([("id", -1)]).limit(1)))[-1]["id"]
    new_id = last_id + 1
    new = {
        "sent": None,
        "arrived": None,
        "created": now,
        "updated": now,
        "transBurs": bet_package["transBurs"],
        "bursExport": bet_package["bursExport"],
        "bursFineMd": bet_package["bursFineMd"],
        "id": new_id,
    }
    result = burs_export_trans.insert_one(new)
    if result.acknowledged is True:
        return
    else:
        log.error(
            f"Expected to add new burs_export_trans for fine md {bet_package['bursFineMd']} but insertion was not acknowledged."
        )
        sys.exit(-1)


def set_newBursExp_to_false(id_val):
    now = right_now()
    trans_burs = arise_mongo("trans_burs")
    new_values = {"$set": {"newBursExp": False, "updated": now}}
    query = {"id": id_val}
    result = trans_burs.update_one(query, new_values)
    if result.modified_count == 1 and result.matched_count == 1:
        return
    else:
        log.error(f"Updated more rows than anticipated on trans_burs's ID {id_val}")
        sys.exit(-1)


def updateBursExportRow(chargesFilename, reversalsFilename, numSent, bursExportId):
    now = right_now()
    burs_exports = arise_mongo("burs_exports")
    new_values = {
        "$set": {
            "chargesFilename": chargesFilename,
            "reversalsFilename": reversalsFilename,
            "numSent": numSent,
            "updated": now,
        }
    }
    query = {"id": bursExportId}
    result = burs_exports.update_one(query, new_values)
    if result.modified_count == 1 and result.matched_count == 1:
        return
    else:
        log.error(f"Updated more rows than anticipated on Burs Export {bursExportId}")
        sys.exit(-1)


def findLatestOneNull(table, column):
    db = arise_mongo(table)
    result = list(db.find({column: None}))
    if len(result) == 0:
        return None
    elif len(result) > 1:
        msg = f"Found more unsent Bursar Export jobs than anticipated (just one) on {table}!"
        log.error(msg)
        raise ValueError(msg)
    else:
        return result[0]


def almaFineId_uiLookup(almaFineId):
    fine_match = findOne("fines", "rootAlmaFineId", almaFineId)
    if fine_match is None:
        return None
    else:
        join_tables = ["fine_mds", "trans_alma", "trans_burs", "item_details"]
        fine_id = fine_match["id"]
        for table in join_tables:
            if table == "fine_mds":
                list_name = "burs_fine_md"
            else:
                list_name = table
            result = findMany(table, "fine", fine_id)
            if result is not None:
                for thing in result:
                    del thing["_id"]
                fine_match[list_name] = result
            else:
                fine_match[list_name] = []
        return fine_match


def insert_payment_import_row(results_dict):
    now = right_now()
    burs_imports = arise_mongo("burs_imports")
    last_import = list(burs_imports.find().sort([("id", -1)]).limit(1))
    if len(last_import) == 0:
        new_id = 1
    else:
        new_id = (last_import[0]["id"]) + 1
    new = {"created": now, "updated": now, "id": new_id, "results": results_dict}
    result = burs_imports.insert_one(new)
    if result.acknowledged is True:
        return new_id
    else:
        log.error(
            f"Expected to add new Fine import {new_id} but insertion was not acknowledged."
        )
        sys.exit(-1)
