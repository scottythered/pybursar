# pyBursar: Alma Bursar Synchronization Service
pyBursar synchronizes two-way passage of library fines & fee data. Direction 1 covers fines logged by library patrons in Alma, which are sent to the Bursar's office. Direction 2 concerns fine payments made by patrons, which are collected from the Bursar's office and marked as paid in Alma.

Currently, pyBursar runs automatically 5 days a week: on M/W/F, both directions are run, and on Tu/Th, only Direction 2 runs.

Due credit goes to Mark McCann, who created the original `alma-bursar-sync` NodeJS app; pyBursar's Direction 1 functionality is a direct translation of Mark's excellent work.

## Essential Instructions For Manual Use
Run `python3.8 abs.py help` for a handy list of commands ready to be copied-n-pasted into the command line.

### Direction 1 (Syncing and Exporting Fines to Bursar)
`python3.8 abs.py exportBurs` runs the necessary processes to export fine/fee data from Alma to the Bursar's office. It encompasses 3 commands which can also be run separately (but absolutely needs to be run in this order):

1. `python3.8 abs.py importAlmaFines`: import new charges and reversals from Alma Analytics.
2. `python3.8 abs.py exportBurs`: process the newly imported transactions and prepare for output to Financial Services.
3. `python3.8 abs.py sftpToBurs`: this is just a check that lists the number of new fines and reversals that will be exported.

If you want to see what transactions will be sent, you will want to view the contents of the newly created files in the `exports` directory.
If there are charges, they will appear in a file ending in `.1`.  If there are reversals, they will appear in a file ending in `.2`.

You after running these processes, you are done with Direction 1, but will want to be on the lookout for an email from the Bursar's office indicating success or failure.
If there were any problems, they will be listed in the e-mail.

If something went wrong in Step 1 (very rare, but we have seen network errors), the best course of action is to wipe out the database and restore to the last successful run. (See below.)

### Direction 2 (Importing Payments From Bursar)
`python3.8 abs.py importAlmaPayments` manually imports fines marked by Financial Services as paid, using the Alma API to register their payment within the Alma system. (Zero-dollar payments and charges are ignored by this process.) This commands runs on a Cron Tu/Th, followed by a backup.

UTO has created a process to make this data available to us on a daily basis as a spreadsheet SFTPed to Steve's data warehouse. Once a year, sometime in November to December, UTO needs to adjust their query to reflect a new "starting from" date and will set an "ending" date to a year form then. (Not sure why they can't have an open-ended query without an ending date, but UTO assured us it has to be this way. Go figure.)

### Combination command
`python3.8 abs.py today` runs Directions 1 and 2 consecutively, followed by a backup. This command runs on a Cron M/W/F.

### Backups
`python3.8 abs.py backupDB` manually creates a complete backup of the pyBursar Mongo database as a compressed file, which is then copied to a local directory. A second copy is sent to an Amazon S3 bucket via a file folder called `~/bursar-sync-s3-backup`, which has been mounted with `s3fs`.

`python3.8 abs.py restoreLast` wipes out the current Mongo DB and restores it from the latest available DB backup file in the local backup directory. `python3.8 abs.py restoreChoose` wipes out the current Mongo DB and asks you to choose a restore point from the 5 latest DB backup files.

### Other useful tools
`python3.8 abs.py lookupFine` checks fine data in the DB by either root Alma Fine ID or by Alma Loan ID. Add field name as a flag (eg, --rootAlmaFineId) and then field value.
Example: `abs.py lookupFine --rootAlmaFineId 7816239430003841`

`python3.8 abs.py printLastExport` shows the last Bursar Export (Direction 1) job run by the app.
