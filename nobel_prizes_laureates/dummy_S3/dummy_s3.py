"""This module is used to test the s3 uploading file in local instead of do it in s3"""
import os
import shutil

class DummyS3:
    """This class have the methods for perform s3 operations in local"""
    
    def __init__(self,config_obj,logger_obj) -> None:
        """This is the init method of the class of DummyS3"""
        self.config = config_obj
        self.logger = logger_obj
        local_s3_path = self.config["local"]["local_s3_path"]
        self.dummy_s3_path = os.path.join(os.path.dirname(os.getcwd()),local_s3_path)
    def upload_dummy_local_s3(self,source,partition_path):  
        """This method will upload the file into dummy local s3 folder"""
        dest = os.path.join(self.dummy_s3_path,partition_path)
        os.makedirs(dest, exist_ok=True)
        shutil.copy(source,dest)
        self.logger.info("File successfully uploade into dummy S3")
        return True
            
    