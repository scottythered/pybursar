import xmltodict
import json
import sys
import os
import pandas as pd
from datetime import *
import paramiko
from fileServices import get_latest_sftp_file
from bursarUtils import billingDateToTermCode, dueDateCloner
from auth_puller import auth_puller
from logger import log
from io import StringIO
from slackMsg import slacker, slackDebug
import requests as r
import mongoPortal
from uiLookup import date_object_converter


def sftp_machine(host, port, username, password):
    """Basic function to call an SFTP service."""
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
    except Exception as e:
        msg = f"SFTP connection error: {e}"
        log.error(msg)
        slacker("SFTP error", msg)
        sys.exit()
    else:
        return paramiko.SFTPClient.from_transport(transport)


def integrity_check(response_one, response_two):
    """Checks the integrity of a file that's been SFTPed. If the stats for the
    local file and the file placed on the SFTP server are the same, it passes
    the integrity check."""
    result = (
        (response_one.st_mode == response_two.st_mode)
        and (response_one.st_uid == response_two.st_uid)
        and (response_one.st_gid == response_two.st_gid)
        and (response_one.st_atime == response_two.st_atime)
        and (response_one.st_mtime == response_two.st_mtime)
    )
    return result


def parse_machine(input_list):
    """Initiates/governs the parsing of incoming fines."""
    parsed_transactions = []
    for fine in input_list:
        patron_id = fine["user"]["value"]
        if type(fine["finefeeList"]["userFineFee"]) == list:
            for userFine in fine["finefeeList"]["userFineFee"]:
                parserator(userFine, parsed_transactions, patron_id)
        elif type(fine["finefeeList"]["userFineFee"]) == dict:
            userFine = fine["finefeeList"]["userFineFee"]
            parserator(userFine, parsed_transactions, patron_id)
    log.info(f"Parsed {len(parsed_transactions)} transactions exported from Alma...")
    return parsed_transactions


def payment_puller():
    xfer = auth_puller("auth.json", "xfer")
    paths = auth_puller("auth.json", "paths")
    sftp = sftp_machine(xfer["host"], xfer["port"], xfer["username"], xfer["password"])
    sftp.chdir(xfer["fromDir"])
    files_directory = sftp.listdir_attr()
    latest_file = get_latest_sftp_file(
        files_directory, "ASUSASF_LIBRARY_REPORT_DTRANGE"
    )
    if latest_file is None:
        msg = "Expected to find at least one payment file from Xfer but SFTP returned none!"
        log.error(msg)
        slacker("Import", msg)
        sys.exit()
    else:
        with sftp.open(latest_file, mode="r") as csv:
            data = csv.read()
        if latest_file in os.listdir(paths["bursImportArchive"]):
            msg = f"SFTP error: latest Bursar payment file {latest_file} already exists in local dir {paths['bursImportArchive']}! Did they stop sending them?"
            log.error(msg)
            slacker("Import", msg)
            sys.exit()
        else:
            latest_file_local_path = os.path.join(
                paths["bursImportArchive"], latest_file
            )
            try:
                sftp.get(
                    os.path.join(xfer["fromDir"], latest_file), latest_file_local_path
                )
            except Exception as e:
                log.error(
                    f"SFTP error on trying to copy Bursar payment file {latest_file}: {e}"
                )
                sftp.close()
                log.info(
                    f"Attempting to dump already-read CSV data to a CSV file instead..."
                )
                try:
                    with open(latest_file_local_path, "w") as f:
                        f.write(str(data, "utf-8"))
                except Exception as e:
                    msg = f"Attempt to dump already-read CSV data to a CSV file failed: {e}"
                    log.error(msg)
                    slacker("Import", msg)
                    sys.exit()
                else:
                    log.info(
                        f"Acquired Bursar payment file {latest_file}, ready to parse..."
                    )
                    return True, latest_file
            else:
                sftp.close()
                log.info(
                    f"Acquired Bursar payment file {latest_file}, ready to parse..."
                )
                return True, latest_file


def payment_parser(pay_file_name):
    paths = auth_puller("auth.json", "paths")
    bursar_pay_files_dir = paths["bursImportArchive"]

    latest_df = pd.read_csv(os.path.join(bursar_pay_files_dir, pay_file_name))

    bursar_pay_files_ls = sorted(os.listdir(bursar_pay_files_dir))
    new_file_index = bursar_pay_files_ls.index(pay_file_name)
    previous_file = bursar_pay_files_ls[(new_file_index - 1)]
    previous_df = pd.read_csv(os.path.join(bursar_pay_files_dir, previous_file))

    dfs_merged = latest_df.merge(
        previous_df,
        on=[
            "Emplid",
            "Date Paid",
            "Reference Number",
            "Charge Amount",
            "Paid Amount",
            "Item Type",
            "Charge Description",
        ],
        how="left",
        indicator=True,
    )
    df_limited = (dfs_merged[dfs_merged["_merge"] == "left_only"]).drop(
        columns=["_merge"]
    )

    if len(df_limited) > 0:
        df_dicts = df_limited.to_dict("records")
        payments = {}
        for entry in df_dicts:
            if entry["Emplid"] not in payments:
                payments[(entry["Emplid"])] = {"payments": []}
                payments[(entry["Emplid"])]["payments"].append(
                    {
                        "bursarTransactionId": str(entry["Reference Number"]),
                        "paidSum": f"{entry['Paid Amount']:0.2f}",
                        "chargeSum": f"{entry['Charge Amount']:0.2f}",
                        "datePaid": str(entry["Date Paid"]),
                    }
                )
            else:
                payments[(entry["Emplid"])]["payments"].append(
                    {
                        "bursarTransactionId": str(entry["Reference Number"]),
                        "paidSum": f"{entry['Paid Amount']:0.2f}",
                        "chargeSum": f"{entry['Charge Amount']:0.2f}",
                        "datePaid": str(entry["Date Paid"]),
                    }
                )

        payments_found = []
        paymentErrorList = []
        for user_id, payments_list in payments.items():
            for payment in payments_list["payments"]:
                fine_searcher = mongoPortal.findOne(
                    "fine_mds", "bursFineId", payment["bursarTransactionId"]
                )
                if fine_searcher is not None:
                    payments_found.append(
                        {
                            "bursarTransactionId": payment["bursarTransactionId"],
                            "almaFineId": fine_searcher["alma_fine_id"],
                            "userID": user_id,
                            "paidSum": payment["paidSum"],
                            "chargeSum": payment["chargeSum"],
                            "datePaid": payment["datePaid"],
                        }
                    )
                else:
                    paymentErrorList.append(
                        {
                            "bursarTransactionId": payment["bursarTransactionId"],
                            "userID": user_id,
                            "paidSum": payment["paidSum"],
                            "chargeSum": payment["chargeSum"],
                            "datePaid": payment["datePaid"],
                        }
                    )

        if len(paymentErrorList) > 0 and len(payments_found) > 0:
            return payments_found, paymentErrorList
        elif len(payments_found) == 0 and len(paymentErrorList) > 0:
            return None, paymentErrorList
        elif len(paymentErrorList) == 0 and len(payments_found) > 0:
            return payments_found, None
    else:
        return None, None


def exl_scanner(parsed_payment_list):
    now = (datetime.now()).strftime("%Y-%m-%d")
    already_transferred_to_alma = []
    succesful_alma_transfers = []
    failed_alma_transfers = []
    unexpected_errors = []
    zero_charges = []
    zero_payments = []
    api_key = auth_puller("auth.json", "exl")
    headers = {"Accept": "application/json"}
    params = {"apikey": api_key, "user_id_type": "all_unique"}
    for payment in parsed_payment_list:
        if payment["chargeSum"] == "0.00":
            payment["message"] = "zero-dollar charge"
            zero_charges.append(payment)
        elif payment["paidSum"] == "0.00":
            payment["message"] = "zero-dollar payment"
            zero_payments.append(payment)
        else:
            check = r.get(
                f"https://api-na.hosted.exlibrisgroup.com/almaws/v1/users/{payment['userID']}/fees/{payment['almaFineId']}",
                headers=headers,
                params=params,
            )
            if check.status_code == 200:
                if check.json()["balance"] >= float(payment["paidSum"]):
                    comment = f"Paid to bursar on {payment['datePaid']}, imported into Alma via alma-bursar-sync on {now}."
                    pay_amount = payment["paidSum"]
                elif check.json()["balance"] < float(payment["paidSum"]):
                    comment = f"Paid to bursar on {payment['datePaid']}, imported into Alma via alma-bursar-sync on {now}. Paid sum was ${payment['paidSum']} but current balance was ${check.json()['balance']}, so current balance was applied as paid amount."
                    pay_amount = f"{check.json()['balance']:0.2f}"
                pay_params = {
                    "apikey": api_key,
                    "op": "pay",
                    "method": "ONLINE",
                    "amount": pay_amount,
                    "comment": comment,
                }
                put_payment = r.post(
                    f"https://api-na.hosted.exlibrisgroup.com/almaws/v1/users/{payment['userID']}/fees/{payment['almaFineId']}",
                    headers=headers,
                    params=pay_params,
                )
                if put_payment.status_code == 200:
                    succesful_alma_transfers.append(payment)
                    log.info(f"Successful payment transfer: {payment['almaFineId']}")
                else:
                    payment["error_message"] = put_payment.json()

                # log.info("Here is where we would transfer the payment via API")
                # succesful_alma_transfers.append(payment)
            else:
                error_json = check.json()
                if (
                    check.status_code == 400
                    and error_json["errorsExist"] is True
                    and "errorList" in error_json
                ):
                    if (
                        len(error_json["errorList"]["error"]) == 1
                        and error_json["errorList"]["error"][0]["errorMessage"]
                        == "Request failed: API does not support closed fees."
                    ):
                        payment["message"] = "API returned fee as closed"
                        already_transferred_to_alma.append(payment)
                    else:
                        payment["error_message"] = error_json
                        unexpected_errors.append(payment)
                else:
                    payment["error_message"] = error_json
                    unexpected_errors.append(payment)
    import_data = {}
    import_data["already_transferred_to_alma"] = already_transferred_to_alma
    import_data["succesful_alma_transfers"] = succesful_alma_transfers
    import_data["failed_alma_transfers"] = failed_alma_transfers
    import_data["unexpected_errors"] = unexpected_errors
    import_data["zero_charge_transfers"] = zero_charges
    import_data["zero_payment_transfers"] = zero_payments
    if (
        len(already_transferred_to_alma) == 0
        and len(succesful_alma_transfers) == 0
        and len(zero_charges) == 0
        and len(zero_payments) == 0
    ) and (len(failed_alma_transfers) > 0 or len(unexpected_errors) > 0):
        return False, import_data
    else:
        return True, import_data


def import_finishing_msg(payment_results_dict, payment_errors):
    msg = "The Alma payment import process has finished."
    if len(payment_results_dict["succesful_alma_transfers"]) > 0:
        successful_transfs = [
            payment["almaFineId"]
            for payment in payment_results_dict["succesful_alma_transfers"]
        ]
        joined = ", ".join(successful_transfs)
        msg += f"\nPayments for {len(payment_results_dict['succesful_alma_transfers'])} fines were processed: {joined}."
    if len(payment_results_dict["zero_charge_transfers"]) > 0:
        zero_charge = [
            payment["almaFineId"]
            for payment in payment_results_dict["zero_charge_transfers"]
        ]
        zg_joined = ", ".join(zero_charge)
        msg += f"\n\nThese fine payments had a zero-dollar charge amount and were not transferred: {zg_joined}"
    if len(payment_results_dict["zero_payment_transfers"]) > 0:
        zero_pay = [
            payment["almaFineId"]
            for payment in payment_results_dict["zero_payment_transfers"]
        ]
        zp_joined = ", ".join(zero_pay)
        msg += f"\n\nThese fine payments had a zero-dollar payment amount and were not transferred: {zp_joined}"
    if len(payment_results_dict["failed_alma_transfers"]) > 0:
        failed = [
            f"{payment['almaFineId']} -- {payment['error_message']}"
            for payment in payment_results_dict["failed_alma_transfers"]
        ]
        f_joined = "; ".join(failed)
        msg += f"\n\nThese fines failed while trying to mark them as paid via the Alma API: {f_joined}"
    if len(payment_results_dict["unexpected_errors"]) > 0:
        unexp = [
            f"{payment['almaFineId']} -- {payment['error_message']}"
            for payment in payment_results_dict["unexpected_errors"]
        ]
        unexp_joined = "; ".join(unexp)
        msg += f"\n\nThese fines failed unexpectedly while their existence in Alma was being checked via the Alma API: {unexp_joined}"
    if payment_errors is not None:
        fines_not_found = [payment["bursarTransactionId"] for payment in payment_errors]
        fnf_joined = ", ".join(fines_not_found)
        msg += f"\n\nThese paid fines could not be located in the local DB during processing: {fnf_joined}"

    if msg == "The Alma payment import process has finished.":
        msg += " No new payments to process today."

    return msg


def printLastExport():
    burs_exports = mongoPortal.arise_mongo("burs_exports")
    last = (list(burs_exports.find().sort([("id", -1)]).limit(1)))[0]
    if "exported_fines" in last:
        del last["exported_fines"]
    del last["_id"]
    print(json.dumps(last, default=date_object_converter, sort_keys=False, indent=4,))


def startNewJob(type):
    now = mongoPortal.right_now()
    jobs = mongoPortal.arise_mongo("jobs")
    last = (list(jobs.find().sort([("id", -1)]).limit(1)))[0]
    new_id = int(last["id"]) + 1
    job = {}
    job["status"] = "In Progress"
    job["type"] = type
    job["scheduled"] = now
    job["started"] = now
    job["created"] = now
    job["updated"] = now
    job["id"] = new_id
    result = jobs.insert_one(job)
    if result.acknowledged is True:
        return new_id
    else:
        log.error(
            f"Expected to add new {type} job #{new_id} but insertion was not acknowledged."
        )


def jobFail(job_id, msg):
    now = mongoPortal.right_now()
    result = mongoPortal.findOne("jobs", "id", int(job_id))
    result["status"] = "Failed"
    result["finished"] = now
    result["errorMsg"] = msg
    result["updated"] = now
    query = {"id": job_id}
    jobs = mongoPortal.arise_mongo("jobs")
    jobs.replace_one(query, result)


def jobWin(job_id):
    now = mongoPortal.right_now()
    result = mongoPortal.findOne("jobs", "id", int(job_id))
    result["status"] = "Success"
    result["finished"] = now
    result["updated"] = now
    query = {"id": job_id}
    jobs = mongoPortal.arise_mongo("jobs")
    jobs.replace_one(query, result)


def errorCaught(slack_heading, message):
    log.error(message)
    slackDebug(slack_heading, message)
