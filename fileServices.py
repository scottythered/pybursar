import os
import shutil
import json
from datetime import *
from bursarUtils import billingDateToTermCode, dueDateCloner
from logger import log
from slackMsg import slacker
import paramiko
from auth_puller import auth_puller
import subprocess


def file_util(op, file_name, origDest, copyDest=None):
    sourcePath = origDest + file_name
    destPath = copyDest + file_name
    if op == "move":
        dest = shutil.move(sourcePath, destPath)
        if dest == destPath:
            log.info(f"{file_name} moved to: {dest}")
        else:
            f"Expected to move {file_name} to storage location {copyDest} but something went wrong"
            log.error(msg)
            slacker("Error", msg)
    elif op == "copy":
        dest = shutil.copy(sourcePath, destPath)
        if dest == destPath:
            log.info(f"{file_name} copied to: {dest}")
        else:
            f"Expected to copy {file_name} to storage location {copyDest} but something went wrong"
            log.error(msg)
            slacker("Error", msg)
    elif op == "delete_dir":
        subprocess.run(["chmod", "-R", "777", f"{origDest}{file_name}"])
        subprocess.run(["rm", "-r", f"{origDest}{file_name}"])


def job_starter(kind):
    temp = {}
    temp["created"] = (datetime.now()).strftime("%Y-%m-%d %I:%M:%S")
    if kind == "export":
        temp["chargesFilename"] = None
        temp["reversalsFilename"] = None
        temp["idsTransferred"] = {}
    elif kind == "import":
        temp["paidFines"] = []
    return temp


def get_latest_sftp_file(sftp_dir_attrib, file_starting_text):
    latest = 0
    newest_file = None

    for fileattr in sftp_dir_attrib:
        if (
            fileattr.filename.startswith(file_starting_text)
            and fileattr.st_mtime > latest
        ):
            latest = fileattr.st_mtime
            newest_file = fileattr.filename

    if newest_file is not None:
        return newest_file
    else:
        return None


def pathExists(thePath):
    return os.path.exists(thePath)


def backupExists(theFile):
    if pathExists(theFile):
        return True
    else:
        return False


def copyFile(sourcePath, destPath):
    dest = shutil.copyfile(sourcePath, destPath)
    if dest == destPath:
        log.info(f"File copied to: {dest}")
        return True


def moveFile(sourcePath, destPath):
    dest = shutil.move(sourcePath, destPath)
    if dest == destPath:
        log.info(f"File moved to: {dest}")
        return True


def bursarFileProcess(file_name):
    paths = auth_puller("auth.json", "paths")
    bursExportDir = os.path.join(paths["local_path"], paths["bursExportArchive"])
    localBursExportArchiveDir = paths["bursExportBackup"]
    remoteBursExportArchiveDir = paths["remoteBursExportBackup"]

    if file_name is not None:
        path = os.path.join(bursExportDir, file_name)
        if pathExists(path):
            result = sftpBursFile(file_name)
            if result:
                log.info(f"SFTP'ed {file_name}")
                copy_dest_path = os.path.join(remoteBursExportArchiveDir, file_name)
                cp_result = copyFile(path, copy_dest_path)
                move_dest_path = os.path.join(localBursExportArchiveDir, file_name)
                mv_result = moveFile(path, move_dest_path)
                if mv_result and cp_result:
                    return True
                else:
                    msg = f"Expected to copy {file_name} to backup location but something went wrong"
                    log.error(msg)
                    raise ValueError(msg)
            else:
                msg = f"Expected to put {file_name} in SFTP location but something went wrong"
                log.error(msg)
                raise ValueError(msg)
        else:
            msg = f"Expected to find file {file_name} in {bursExportDir} but dir is not there"
            log.error(msg)
            raise FileNotFoundError(msg)
    else:
        log.info(f"Filename is None, no file to send")
        return True


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
    result = response_one.st_size == response_two.st_size
    return result


def sftpBursFile(file):
    burs = auth_puller("auth.json", "bursar_sftp")
    paths = auth_puller("auth.json", "paths")
    sftp = sftp_machine(burs["host"], burs["port"], burs["username"], burs["pw"])
    local_path = os.path.join(paths["local_path"], paths["bursExportArchive"], file)
    upload_path = os.path.join(burs["bursarUpload"], file)
    try:
        put_file = sftp.put(local_path, upload_path)
    except Exception as e:
        msg = f"SFTP to Bursar error on {file}: {e}"
        log.error(msg)
        slacker("Export", msg)
        sys.exit()
    else:
        stat_local_file = os.stat(local_path)
        if integrity_check(put_file, stat_local_file):
            return True
        else:
            msg = f"SFTP transfer integrity check failed on {file} size, consider checking it out!"
            log.error(msg)
            slacker("SFTP transfer", msg)
            return True
