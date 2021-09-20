import requests as r
import xmltodict
import pandas as pd
import json
from auth_puller import auth_puller


reportsPrefix = "/shared/Arizona State University/Reports/Synchronization/"
fineStatsPath = reportsPrefix + "Fine Summary Stats"
allFinesPath = reportsPrefix + "All Fines"
allTransactionsPath = reportsPrefix + "All Transactions"
allUserBalancesPath = reportsPrefix + "All User Balances"


def list_builder(rows, list_added_to):
    if isinstance(rows, list):
        for row in rows:
            list_added_to.append(row)
    elif isinstance(rows, dict):
        list_added_to.append(rows)


def getAlmaTable(path, filtered):
    apikey = auth_puller("auth.json", "exl")
    maxRows = 0
    finished = False
    token = ""
    temp_list = []

    while not finished:
        maxRows += 1000
        print(f"Grabbing rows {maxRows}...")
        if token != "":
            params = (
                ("limit", 1000),
                ("apikey", apikey),
                ("token", token),
            )
        else:
            if filtered == "none":
                params = (
                    ("limit", maxRows),
                    ("apikey", apikey),
                    ("path", path),
                )
            else:
                params = (
                    ("limit", maxRows),
                    ("apikey", apikey),
                    ("path", path),
                    ("filter", filtered),
                )

        headers = {"accept": "application/xml", "Accept-Charset": "UTF-8"}
        response = r.get(
            "https://api-na.hosted.exlibrisgroup.com/almaws/v1/analytics/reports",
            headers=headers,
            params=params,
        )
        xml_dict = xmltodict.parse(response.text)
        json_decoder = json.loads(json.dumps(xml_dict))
        if "report" not in json_decoder:
            path_kind = (path.split("/"))[-1]
            log.error(
                f"HTTP request to ExL's analytics for the {path_kind} report returned no report. Analytics access is probably (almostg certainly) down. Try again in 5 minutes."
            )
            sys.exit(-1)
        else:
            rows = json_decoder["report"]["QueryResult"]["ResultXml"]["rowset"]["Row"]

            if json_decoder["report"]["QueryResult"]["IsFinished"] != "true":
                list_builder(rows, temp_list)
                if (
                    "ResumptionToken" in json_decoder["report"]["QueryResult"]
                    and token == ""
                ):
                    token = json_decoder["report"]["QueryResult"]["ResumptionToken"]
                else:
                    pass
            else:
                list_builder(rows, temp_list)
                finished = True

    df = pd.DataFrame(temp_list)
    return df


def getAllFines(geDate):
    date_filter = f'<sawx:expr xsi:type="sawx:comparison" op="greaterOrEqual" xmlns:saw="com.siebel.analytics.web/report/v1.1" xmlns:sawx="com.siebel.analytics.web/expression/v1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><sawx:expr xsi:type="sawx:sqlExpression">"Fines and Fees Transactions"."Status Date"</sawx:expr><sawx:expr xsi:type="xsd:dateTime">{geDate}</sawx:expr></sawx:expr>'
    new_df = getAlmaTable(allFinesPath, date_filter)
    if len(new_df) > 0:
        new_df.rename(
            columns={
                "Column0": "0",
                "Column1": "Fine Comment",
                "Column2": "Fine Fee Creation Date",
                "Column3": "Fine Fee Id",
                "Column4": "Fine Fee Status",
                "Column5": "Fine Fee Type",
                "Column6": "Status Date",
                "Column7": "Author",
                "Column8": "Barcode",
                "Column9": "Call Number",
                "Column10": "Due Date",
                "Column11": "Due Time",
                "Column12": "Item Loan Id",
                "Column13": "Loan Date",
                "Column14": "Loan Time",
                "Column15": "Return Date",
                "Column16": "Return Time",
                "Column17": "Loan Details",
                "Column18": "Primary Identifier",
                "Column19": "law-stks ELSE Location Code",
                "Column20": "Original Amount",
                "Column21": "Remaining Amount",
            },
            inplace=True,
        )
    new_df.drop(columns=["0"], inplace=True)
    new_df.where(new_df.notnull(), None, inplace=True)
    return new_df


def getAllTransactions(geDate):
    date_filter = f'<sawx:expr xsi:type="sawx:comparison" op="greaterOrEqual" xmlns:saw="com.siebel.analytics.web/report/v1.1" xmlns:sawx="com.siebel.analytics.web/expression/v1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><sawx:expr xsi:type="sawx:sqlExpression">"Fines and Fees Transactions"."Fine Fee Transaction Creation Date"</sawx:expr><sawx:expr xsi:type="xsd:dateTime">{geDate}</sawx:expr></sawx:expr>'
    new_df = getAlmaTable(allTransactionsPath, date_filter)
    if len(new_df) > 0:
        new_df.rename(
            columns={
                "Column0": "0",
                "Column1": "Fine Fee Id",
                "Column2": "Fine Fee Transaction Creation Date",
                "Column3": "Fine Fee Transaction Id",
                "Column4": "Fine FeeTransaction Type",
                "Column5": "Transaction Note",
                "Column6": "Item Loan Id",
                "Column7": "Primary Identifier",
                "Column8": "Transaction Amount",
            },
            inplace=True,
        )
    new_df.drop(columns=["0"], inplace=True)
    new_df.where(new_df.notnull(), None, inplace=True)
    return new_df


def getAllUserBalances():
    new_df = getAlmaTable(allUserBalancesPath, "none")
    new_df.rename(
        columns={
            "Column0": "0",
            "Column1": "Primary Identifier",
            "Column2": "Remaining Amount",
        },
        inplace=True,
    )
    new_df.drop(columns=["0"], inplace=True)
    new_df.where(new_df.notnull(), None, inplace=True)
    return new_df
