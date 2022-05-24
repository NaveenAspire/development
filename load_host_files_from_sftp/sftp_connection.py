"""This module that is used for connect the sftp"""
import pysftp


class SftpCon:
    """This is the class that contains methods for get file from sftp and upload to s3"""

    def __init__(self, config_obj, logger_obj):
        """This is the init method of the class of SftpCon"""
        self.conn = pysftp.Connection(
            host=config_obj["SFTP"]["host"],
            username=config_obj["SFTP"]["username"],
            password=config_obj["SFTP"]["password"],
        )
        self.r_path = (config_obj["SFTP"]["remote_path"],)
        self.logging = logger_obj

    def list_files(self):
        """This method that returns the list of files names for the given path"""
        try:
            sftp_file_list = self.conn.listdir(self.r_path)
            self.logging.info("Successfully get the sftp file list...")
        except Exception as err:
            print(err)
            self.logging.error("Error occured while getting sftp file list as %s",err)
            sftp_file_list = None
        return sftp_file_list

    def get_new_file_only(self, lpath, file_exist_list):
        """This method that retrieve the new files created in server only"""
        try:
            sftp_files = self.list_files()
            files = [file for file in sftp_files if file not in file_exist_list]
            for file in files:
                self.conn.get(self.r_path + "/" + file, lpath + "/" + file)
            self.conn.close()
            self.logging.info("Successfully get new file list...")
        except Exception as err:
            self.logging.error("Error occured while getting new files : %s", err)
            print(err)
            lpath = None
        return lpath
