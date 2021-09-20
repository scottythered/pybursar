from datetime import datetime
import sys
import subprocess
import os
import json
from logger import log
from auth_puller import auth_puller
from fileServices import file_util, pathExists
from slackMsg import slacker
from mongoPortal import instantiate_almabursar_DB


def backup():
    log.info("Backup initiated.")

    paths = auth_puller("auth.json", "paths")

    now = datetime.now()

    backup_file_name = f"alma_bursar_dev_after_BursExport_{now.strftime('%Y-%m-%d')}.gz"

    dbLocalBackup_path = paths["dbLocalBackup"]
    dbRemoteBackup_path = paths["dbRemoteBackup"]
    dbWorkingBackup_path = os.path.join(paths["local_path"], paths["workingBackup"])

    dbLocalBackup_file_path = os.path.join(dbLocalBackup_path, backup_file_name)

    if pathExists(dbLocalBackup_file_path):
        backup_file_name = (
            f"alma_bursar_dev_after_BursExport_{now.strftime('%Y-%m-%dT%H%M%S')}.gz"
        )

    dbZippedPath = os.path.join(dbWorkingBackup_path, backup_file_name)

    # dumping the backup
    try:
        completed = subprocess.run(
            ["mongodump", "-d", "almabursar", "--gzip", f"--archive={dbZippedPath}"]
        )
    except Exception as e:
        msg = f"Error during backup: {e}"
        log.error(msg)
        slacker("Backup Process", msg)
        sys.exit(-1)
    else:
        try:
            # copy
            file_util(
                "copy", backup_file_name, dbWorkingBackup_path, dbRemoteBackup_path,
            )
        except Exception as e:
            msg = f"Error during backup copy: {e}"
            log.error(msg)
            slacker("Backup Process", msg)
            sys.exit(-1)
        try:
            # move
            file_util(
                "move", backup_file_name, dbWorkingBackup_path, dbLocalBackup_path,
            )
        except Exception as e:
            msg = f"Error during backup move: {e}"
            log.error(msg)
            slacker("Backup Process", msg)
            sys.exit(-1)
        msg = "Backup successfully generated."
        log.info(msg)
        slacker("Backup Process", msg)
        sys.exit(0)


def wipe_out():
    log.info("Wiping collections from Almabursar...")
    almabursar = instantiate_almabursar_DB()
    collecs = almabursar.list_collection_names()
    counter = 0
    for coll in collecs:
        collection_to_wipe = almabursar[coll]
        collection_to_wipe.drop()
        counter += 1
    log.info(f"Wiped {counter} Almabursar DB collections.")


def last_five_backups(list_of_file_paths):
    temp = []
    for path in list_of_file_paths:
        ctime = os.path.getctime(path)
        file = path.split("/")[-1]
        temp.append({"path": path, "file": file, "ctime": ctime})
    choices = {}
    enum = 0
    for working_dict in sorted(temp, key=lambda i: i["ctime"], reverse=True)[0:5]:
        enum += 1
        choices[enum] = working_dict
    return choices


def restore_from_backup(backup_file_name, backup_full_path):
    try:
        log.info(
            f"Restoring Almabursar DB from most recent backup file ({backup_file_name})"
        )
        completed = subprocess.run(
            [
                "mongorestore",
                "--nsInclude=almabursar.*",
                "--gzip",
                f"--archive={backup_full_path}",
            ]
        )
    except Exception as e:
        msg = f"Error while trying to restore DB from file {backup_file_name}: {e}"
        log.error(msg)
        slacker("DB Restore From Backup", msg)
        sys.exit(-1)
    else:
        msg = f"Successfully restored DB from file {backup_file_name}."
        log.info(msg)
        slacker("DB Restore From Backup", msg)
        sys.exit(0)


def restore_last_archive():
    paths = auth_puller("auth.json", "paths")
    if pathExists(paths["dbLocalBackup"]):
        backup_files = os.listdir(paths["dbLocalBackup"])
        joined_paths = [
            os.path.join(paths["dbLocalBackup"], basename) for basename in backup_files
        ]
        latest_backup = max(joined_paths, key=os.path.getctime)
        backup_name = latest_backup.split("/")[-1]
        wipe_out()
        restore_from_backup(backup_name, latest_backup)
    else:
        msg = f"Could not find the local directory of backup files Does it not exist?"
        log.error(msg)
        slacker("DB Restore From Backup", msg)
        sys.exit(-1)


def restore_choose():
    paths = auth_puller("auth.json", "paths")
    if pathExists(paths["dbLocalBackup"]):
        backup_files = os.listdir(paths["dbLocalBackup"])
        joined_paths = [
            os.path.join(paths["dbLocalBackup"], basename) for basename in backup_files
        ]
        choices = last_five_backups(joined_paths)
        print("\n")
        for key in choices:
            print(f"{key}: {choices[key]['file']}")
        print("\n")
        while True:
            try:
                chosen = int(
                    input(
                        "Please enter the number next to the backup file you want to use as a restore point: "
                    )
                )
                if 0 < chosen < 6:
                    break
                print("Try again, but enter a number from the list above.")
            except ValueError:
                print("C'mon, I said enter a number.")
        wipe_out()
        restore_from_backup(choices[chosen]["file"], choices[chosen]["path"])
    else:
        msg = f"Could not find the local directory of backup files Does it not exist?"
        log.error(msg)
        slacker("DB Restore From Backup", msg)
        sys.exit(-1)
