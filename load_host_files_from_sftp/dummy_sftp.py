"""This module will create the dummy sftp path and methods to retrieve the files"""
import os
import shutil
import numpy as np


class DummySftp:
    """This is the class for dummy sftp"""

    def __init__(self, config_obj, logger_obj) -> None:
        """This is the init method for the class DummySftp"""
        self.r_path = config_obj["local"]["local_sftp_rpath"]
        self.logging = logger_obj
        local_sftp_path = config_obj["local"]["local_sftp_path"]
        self.dummy_sftp_path = os.path.join(
            os.path.dirname(os.getcwd()), local_sftp_path
        )
        self.dest_rpath = os.path.join(self.dummy_sftp_path, self.r_path)
        os.makedirs(self.dummy_sftp_path, exist_ok=True)

    def list_files(self):
        """This method will return the list of files in the sftp path"""
        try:
            sftp_file_list = os.listdir(self.dest_rpath)
        except Exception as err:
            print(err)
            sftp_file_list = None
        return sftp_file_list

    def get_new_file_only(self, lpath, file_exist_list):
        """This method that retrieve the new files created in server only"""
        try:
            sftp_files = self.list_files()
            files = list(np.setdiff1d(sftp_files,file_exist_list))
            if not files:
                print("There are no new files...")
                raise ValueError
            print(f"New Files are : {files}")
            for file in files:
                shutil.copy(self.dest_rpath + "/" + file, lpath + "/" + file)
        except Exception as err:
            self.logging.error("Error occured : %s", err)
            print(err)
            lpath = None
        return lpath
