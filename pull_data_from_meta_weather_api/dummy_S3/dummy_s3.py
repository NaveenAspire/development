"""This module is used to test the s3 uploading file in local instead of do it in s3"""
import os
import shutil

class DummyS3:
    """This class have the methods for perform s3 operations in local"""
    
    def __init__(self,config_obj) -> None:
        """This is the init method of the class of DummyS3"""
        self.config = config_obj
    def upload_dummy_local_s3(self):
        for file in os.listdir(self.path):
            partition_path = self.get_partition_path(file)
            local_s3_path = self.config["local"]["local_s3_path"]
            dummy_s3 = local_s3_path + partition_path
            print(dummy_s3)
            os.makedirs(dummy_s3, exist_ok=True)
            shutil.copy(self.path + file, dummy_s3)
    