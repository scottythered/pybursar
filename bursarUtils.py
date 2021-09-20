from datetime import *
from dateutil.relativedelta import *
from auth_puller import auth_puller
from logger import log
import os


def formatBursDate(ts):
    """Returns a date formated as MMDDYYYY if date is not null, otherwise
    the empty string. This is used for writing to the bursar export file.
    """
    if ts is None:
        return ""
    else:
        return (ts).strftime("%m%d%Y")


def formatBursReversal(isNegAmt):
    if isNegAmt < 0:
        return "Y"
    else:
        return ""


def billingDateToTermCode(date_data):
    """1 to 5 = Jan to May = Spring = 1
    6 to 7 = Jun to Jul = Summer = 4
    8 to 12 = Aug to Dec = Fall = 7
    """
    if date(date_data.year, 1, 1) <= date_data <= date(date_data.year, 5, 31):
        t_code = "1"
    elif date(date_data.year, 6, 1) <= date_data <= date(date_data.year, 7, 31):
        t_code = "4"
    elif date(date_data.year, 8, 1) <= date_data <= date(date_data.year, 12, 31):
        t_code = "7"
    return "2" + str(date_data.year)[2:4] + t_code


def formatField(prop, val):
    """Formats the fields contained in bursTransToOasisLine()."""
    fieldPad = " "
    if val is None:
        val = ""
    if len(val) > prop["length"]:
        raise ValueError(
            f"Value {val} length given for bursar field {prop['fieldName']} is bigger than its length ({str(prop['length'])} characters)"
        )
    if prop["justify"] == "right":
        return val.rjust(prop["length"], fieldPad)
    else:
        return val.ljust(prop["length"], fieldPad)


def dueDateCloner(today):
    """Export file due dates are the 25th of the next month, or the 25th of
    the month after next if the current day number is > 25.
    """
    day_number = today.day
    if day_number > 25:
        two_months = today + relativedelta(months=+2)
        return datetime(two_months.year, two_months.month, 25)
    else:
        next_month = today + relativedelta(months=+1)
        return datetime(next_month.year, next_month.month, 25)


def bursTransToOasisLine(asuId, chargeAcc, amt, dueDate, billingDate, termCode, refId):
    """Transforms raw data into the line format necessary for the bursar's
    office to process fines.
    """
    # eg: '1012345678 ' -- this does not link to a real patron
    patronIdProp = {
        "fieldNum": 1,
        "fieldName": "Patron id",
        "length": 11,
        "justify": "left",
    }

    # eg: '531000000001'
    accountNumProp = {
        "fieldNum": 2,
        "fieldName": "Library payment account number",
        "length": 12,
        "justify": "left",
    }

    # eg: '     65.00'
    billingAmtProp = {
        "fieldNum": 3,
        "fieldName": "Billing amount (unsigned)",
        "length": 10,
        "justify": "right",
    }

    # Normally set to the 25th of the following month.
    # Format: MMDDYYYY
    # eg: '07252017 '
    fineDueDateProp = {
        "fieldNum": 4,
        "fieldName": "Fine due date",
        "length": 9,
        "justify": "left",
    }

    # The creation/billing date of the fine
    # Format: MMDDYYYY
    # eg: '06232017 '
    billingDateProp = {
        "fieldNum": 5,
        "fieldName": "Fine billing date",
        "length": 9,
        "justify": "left",
    }

    # Format: 2YYT where YY is last two digits of billing year
    # and T is one of: '1' for spring term, '4' for summer term
    # and '7' for fall term. T corresponds to billing date.
    # eg: '2174  '
    yearTermCodeProp = {
        "fieldNum": 6,
        "fieldName": "Year and term code for billing date",
        "length": 5,
        "justify": "left",
    }

    # 'Y  ' for a reversal, ie. a waive or reverse charge.
    # '   ' for a normal charge.
    # eg: 'Y  '
    isReversalCodeProp = {
        "fieldNum": 7,
        "fieldName": "Code to indicate if a reversal / sign of amt",
        "length": 3,
        "justify": "left",
    }

    # length seems to be 31 in practice
    # eg: '364089_bA19002230799_01        '
    fineRefNumProp = {
        "fieldNum": 8,
        "fieldName": "Fine reference number in PeopleSoft",
        "length": 31,
        "justify": "left",
    }

    replRefNumPostifx = "01"
    fineRefNumPostfix = "03"
    fieldCount = 8
    rowLength = 97

    # string to seperate fields
    fieldSep = " "
    # character to pad each field to its fixed length

    r = ""
    r += formatField(patronIdProp, asuId) + fieldSep
    r += formatField(accountNumProp, chargeAcc) + fieldSep
    r += formatField(billingAmtProp, f"{abs(amt):.2f}") + fieldSep
    r += formatField(fineDueDateProp, formatBursDate(dueDate)) + fieldSep
    r += formatField(billingDateProp, formatBursDate(billingDate)) + fieldSep
    r += formatField(yearTermCodeProp, termCode) + fieldSep
    r += formatField(isReversalCodeProp, formatBursReversal(amt)) + fieldSep
    r += formatField(fineRefNumProp, refId)

    return r


def export_builder(transac_list, doc_type, jobNum, jobDict):
    """Builds, then exports reversals/charges docs."""
    paths = auth_puller("auth.json", "paths")

    if len(transac_list) > 0:
        export_lines = [
            bursTransToOasisLine(
                entry["patron_id"],
                entry["chargeAcc"],
                entry["amt"],
                entry["billDueDate"],
                entry["billingDate"],
                entry["termCode"],
                entry["bursarTransactionId"],
            )
            for entry in transac_list
        ]

        file_dump = "\n".join(export_lines)

        datum = (datetime.now()).strftime("%Y%m%d")

        if doc_type == "reversals":
            file_name = f"LIB_charges.dat.{datum}.{jobNum}.2"
            jobDict["reversalsFilename"] = file_name
            jobDict["idsTransferred"]["reversals"] = [
                entry["bursarTransactionId"] for entry in transac_list
            ]
        elif doc_type == "charges":
            file_name = f"LIB_charges.dat.{datum}.{jobNum}.1"
            jobDict["chargesFilename"] = file_name
            jobDict["idsTransferred"]["charges"] = [
                entry["bursarTransactionId"] for entry in transac_list
            ]

        file_path = os.path.join(
            paths["local_path"], paths["bursExportArchive"], file_name
        )

        with open(file_path, "w") as f:
            f.write(file_dump)

        jobDict["updated"] = (datetime.now()).strftime("%Y-%m-%d %I:%M:%S")
        log.info(f"Dumped {len(export_lines)} {doc_type} to file {file_name}")
        return len(export_lines)
    else:
        jobDict["updated"] = (datetime.now()).strftime("%Y-%m-%d %I:%M:%S")
        return 0
