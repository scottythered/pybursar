from backup import backup
from importAnalyticsData import importAlmaTransactions
from exportToBursar import bursarExport, sendBursExportFile


def main():
    import_result = importAlmaTransactions()
    if import_result:
        export_result = bursarExport()
        if export_result:
            sendBursExportFile()
        backup()


if __name__ == "__main__":
    main()
