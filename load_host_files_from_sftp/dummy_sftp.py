"""This module will create the dummy sftp path and methods to retrieve the files"""
import os
import shutil


class DummySftp:
    """This is the class for dummy sftp"""

    def __init__(self, config_obj, logger_obj) -> None:
        """This is the init method for the class DummySftp"""
        self.logging = logger_obj
        local_sftp_path = config_obj["local"]["local_sftp_path"]
        self.dummy_sftp_path = os.path.join(
            os.path.dirname(os.getcwd()), local_sftp_path
        )
        os.makedirs(self.dummy_sftp_path, exist_ok=True)

    def list_files(self, rpath):
        """This method will return the list of files in the sftp path"""
        try:
            dest_rpath = os.path.join(self.dummy_sftp_path, rpath)
            sftp_file_list = os.listdir(dest_rpath)
        except Exception as err:
            print(err)
            sftp_file_list = None
        return sftp_file_list

    def get_file(self, file_path, lpath, file):
        """This method will download the file which you
        pass as param in the local path given in param"""
        try:
            dest_rpath = os.path.join(self.dummy_sftp_path, file_path)
            shutil.copy(dest_rpath, lpath + "/" + file)
        except Exception as err:
            print(err)
            error = err
            self.logging.error("Error occured while downloading file : %s", err)
        return not "error" in locals()
