from datetime import date
import sys
import uiLookup
import jobUtils
from backup import backup, restore_last_archive, restore_choose
from importAnalyticsData import importAlmaTransactions
from exportToBursar import bursarExport, sendBursExportFile
from bursarFinePayments import importFinePaymentData


def help_me():
    """Print a usage statement for this script."""
    print("This handy list of commands can be copied-&-pasted into the command")
    print("line as argvs to this Python script.")
    print("Example:")
    print("   python3.8 abs.py [command]")
    print(" ")
    print("today -- runs the complete series of daily tasks in this order:")
    print("   - importAlmaFines")
    print("   - exportBurs")
    print("   - sftpToBurs")
    print("   - importAlmaPayments")
    print("   - backupDB")
    print(" ")
    print("payFines -- runs only the fine payments and backup commands.")
    print(" ")
    print("These commands can also be run manually:")
    print(" ")
    print("importAlmaFines -- manually imports new charges and reversals from")
    print("   Alma Analytics.")
    print(" ")
    print("exportBurs -- manually processes the newly imported transactions and")
    print("   prepares for output to Financial Services.")
    print(" ")
    print("   If you want to see what transactions will be sent, you will want")
    print("   to view the contents of the newly created files in the `exports`")
    print("   directory before running the SFTP command. If there are charges,")
    print("   they will appear in a file ending with .1; if there are reversals,")
    print("   they will appear in a file ending with .2.")
    print(" ")
    print("sftpToBurs -- manually SFTPs the files created in 'exportBurs' to a")
    print("   folder in general.asu.edu that is picked up by Financial")
    print("   Services daily. Also archives copies of the transaction files")
    print("   into local & remote directories.")
    print(" ")
    print("importAlmaPayments -- manually imports fines marked by Financial")
    print("   Services as paid and uses the Alma API to register their payment.")
    print("   within the ALma system. (Zero-dollar payments and charges are")
    print("   ignored by this process.)")
    print(" ")
    print("backupDB -- manually creates a complete backup of the alma_bursar_sync")
    print("   database as a compressed file, which is then copied to local and")
    print("   remote archive directories.")
    print(" ")
    print("Other useful tools:")
    print(" ")
    print("lookupFine -- checks fine data in the DB by either root Alma Fine ID")
    print("    or by Alma Loan ID. Add field name as a flag (eg, --rootAlmaFineId)")
    print("    and then field value.")
    print("    Example: abs.py lookupFine --rootAlmaFineId 7816239430003841")
    print(" ")
    print("restoreLast -- wipes out the current Mongo DB and restores it from the")
    print("    latest available DB backup file.")
    print(" ")
    print("restoreChoose -- wipes out the current Mongo DB and asks you to")
    print("    choose a restore point from the 5 latest DB backup files.")
    print(" ")
    print("printLastExport -- see the last Bursar Export job run by the app.")
    print(" ")
    sys.exit(1)


def main(argv):
    if len(argv) > 2 and argv[1] == "lookupFine":
        if argv[2] != "--rootAlmaFineId" and argv[2] != "--almaLoanId":
            print("--rootAlmaFineId or --almaLoanId required for lookupFine command.")
            sys.exit(-1)
        else:
            command = argv[2]
            if command == "--rootAlmaFineId":
                uiLookup.lookupFineByRootAlmaFineId(argv[-1])
                sys.exit(1)
            elif command == "--almaLoanId":
                uiLookup.lookupFineByAlmaLoanId(argv[-1])
                sys.exit(1)

    elif len(argv) > 2 and argv[1] != "lookupFine":
        print("Too many lookup arguments given.")
        sys.exit(-1)

    else:
        if len(argv) == 2 and argv[1] == "today":
            importAlmaTransactions()
            bursarExport()
            sendBursExportFile()
            importFinePaymentData()
            backup()
            sys.exit(1)

        elif len(argv) == 2 and argv[1] == "payFines":
            importFinePaymentData()
            backup()
            sys.exit(1)

        elif (len(argv) == 1 and argv[0] == "abs.py") or (
            len(argv) == 2 and argv[1] == "help"
        ):
            help_me()

        elif argv[1] == "importAlmaPayments":
            importFinePaymentData()
            sys.exit(1)

        elif argv[1] == "importAlmaFines":
            importAlmaTransactions()
            sys.exit(1)

        elif argv[1] == "exportBurs":
            bursarExport()
            sys.exit(1)

        elif argv[1] == "sftpToBurs":
            sendBursExportFile()
            sys.exit(1)

        elif argv[1] == "backupDB":
            backup()
            sys.exit(1)

        elif argv[1] == "printLastExport":
            jobUtils.printLastExport()
            sys.exit(1)

        elif argv[1] == "restoreLast":
            restore_last_archive()
            sys.exit(1)

        elif argv[1] == "restoreChoose":
            restore_choose()
            sys.exit(1)

        else:
            print(
                "Unknown command given. Try running 'abs.py today' to see viable commands."
            )
            sys.exit(-1)


if __name__ == "__main__":
    main(sys.argv)
