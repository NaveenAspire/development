"""This module that is used for connect the sftp"""
import pysftp
import numpy as np


class SftpCon:
    """This is the class that contains methods for get file from sftp and upload to s3"""

    def __init__(self, config_obj, logger_obj):
        """This is the init method of the class of SftpCon"""
        self.conn = pysftp.Connection(
            host=config_obj["SFTP"]["host"],
            username=config_obj["SFTP"]["username"],
            password=config_obj["SFTP"]["password"],
        )
        self.r_path = (config_obj["load_host_files"]["sftp_rpath"],)
        self.logging = logger_obj

    def list_files(self):
        """This method that returns the list of files names for the given path"""
        try:
            sftp_file_list = self.conn.listdir(self.r_path)
            self.logging.info("Successfully get the sftp file list...")
        except Exception as err:
            print(err)
            self.logging.error("Error occured while getting sftp file list as %s", err)
            sftp_file_list = None
        return sftp_file_list

    def get_file(self, rpath, lpath):
        """This method will download the file which you
        pass as param in the local path given in param"""
        try:
            self.conn.get(rpath, lpath)
        except Exception as err:
            print(err)
            error = err
            self.logging.error("Error occured while downloading file : %s", err)
        return not "error" in locals()
