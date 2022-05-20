"""This module that is used for connect the sftp"""
import pysftp

class SftpCon:
    """This is the class that contains methods for get file from sftp and upload to s3"""

    def __init__(self,config_obj,logger_obj):
        """This is the init method of the class of SftpCon"""
        self.conn = pysftp.Connection(
            host=config_obj["SFTP"]["host"],
            username=config_obj["SFTP"]["username"],
            password=config_obj["SFTP"]["password"],
        )
        self.r_path = config_obj["SFTP"]["remote_path"],
        self.logging = logger_obj

    def get_all_files(self,lpath):
        """This method that retrieve the all files in server only"""
        try :
            self.conn.get_d(self.r_path,lpath)
            self.conn.close()
            response = True
        except Exception as err :
            print(err)
            self.logging.error("While call get_d in sftp error occured as %s",err)
            response = False
        return response
