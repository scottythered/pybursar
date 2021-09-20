from mongoPortal import insert_payment_import_row
from jobUtils import (
    startNewJob,
    payment_puller,
    payment_parser,
    exl_scanner,
    import_finishing_msg,
    jobWin,
    jobFail,
)
from logger import log
from slackMsg import slacker


def importFinePaymentData():
    log.info("Alma fine payment import process started...")
    payments_status, new_pay_file_name = payment_puller()
    if payments_status:
        parsed_payments, payment_parse_errors = payment_parser(new_pay_file_name)
        if parsed_payments is not None:
            log.info(
                f"Parsed {len(parsed_payments)} fine payments from Bursar to import into Alma..."
            )
            job_num = startNewJob("BursarPayments")
            payment_results, payment_result_data = exl_scanner(parsed_payments)
            if payment_results:
                jobWin(job_num)
                insert_payment_import_row(payment_result_data)
                msg = import_finishing_msg(payment_result_data, payment_parse_errors)
                slacker("Payment Import", msg)
                log.info(msg)
            else:
                msg = f"Error during transfer of payments to Alma: there were more errors than successful transfers. See data:\n{payment_result_data}"
                jobFail(job_num, msg)
                slacker("Payment Import", msg)
                log.error(msg)
        else:
            if payment_parse_errors is not None:
                joined = ", ".join(payment_parse_errors)
                msg = f"Error -- all of the Imported payments could not be found during processing: {joined}"
                log.error(msg)
                slacker("Payment Import", msg)
            else:
                msg = f"Alma Import process has finished, no new payments to import."
                log.info(msg)
                slacker("Payment Import", msg)


if __name__ == "__main__":
    alma_import()
