import json
import pymongo
from bson.objectid import ObjectId
import datetime
import datetime
from logger import log
from mongoPortal import arise_mongo, almaFineId_uiLookup


def fine_Lookup(fine_field, fine_id):
    fines = arise_mongo("fines")
    matching_fines = list(fines.find({fine_field: str(fine_id)}))
    if len(matching_fines) == 0:
        return None
    else:
        return matching_fines


def date_object_converter(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()


def lookupFineByRootAlmaFineId(rootAlmaFineId):
    fine_data = fine_Lookup("rootAlmaFineId", rootAlmaFineId)
    if fine_data is not None:
        if len(fine_data) == 1:
            total_package = almaFineId_uiLookup(rootAlmaFineId)
            print(f"Fine with rootAlmaFineId {rootAlmaFineId} found:")
            print(
                json.dumps(
                    fine_obj_deserialize(total_package),
                    default=date_object_converter,
                    sort_keys=False,
                    indent=4,
                )
            )
        elif len(fine_data) > 1:
            print(
                f"WARNING: {len(fine_data)} fines with rootAlmaFineId {rootAlmaFineId} found:"
            )
            print(
                json.dumps(
                    fine_obj_deserialize(fine_data),
                    default=date_object_converter,
                    sort_keys=False,
                    indent=4,
                )
            )
    else:
        print(f"No fine with rootAlmaFineId {rootAlmaFineId} found.")


def lookupFineByAlmaLoanId(almaLoanId):
    fine_data = fine_Lookup("almaLoanId", almaLoanId)
    if fine_data is not None:
        if len(fine_data) == 1:
            print(f"Fine with almaLoanId {almaLoanId} found:")
            print(
                json.dumps(
                    fine_obj_deserialize(fine_data),
                    default=date_object_converter,
                    sort_keys=False,
                    indent=4,
                )
            )
        elif len(fine_data) > 1:
            print(f"{len(fine_data)} fines with rootAlmaFineId {almaLoanId} found:")
            print(
                json.dumps(
                    fine_obj_deserialize(fine_data),
                    default=date_object_converter,
                    sort_keys=False,
                    indent=4,
                )
            )
    else:
        print(f"No fines with almaLoanId {almaLoanId} found.")


def fine_obj_deserialize(input_pack):
    if type(input_pack) == list:
        for fine in input_pack:
            fine["_id"] = str(fine["_id"])
            if "bursar_export_id" in fine:
                fine["bursar_export_id"] = str(fine["bursar_export_id"])
    else:
        input_pack["_id"] = str(input_pack["_id"])
    return input_pack
