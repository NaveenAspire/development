"""This module is used to test the s3 uploading file in local instead of do it in s3"""
import os
import shutil


class TempS3:
    """This class have the methods for perform s3 operations in local"""

    def __init__(self, config_obj, logger_obj) -> None:
        """This is the init method of the class of DummyS3"""
        self.config = config_obj
        self.logging = logger_obj
        local_s3_path = self.config["local"]["local_s3_path"]
        self.local_s3_path = os.path.join(os.path.dirname(os.getcwd()), local_s3_path)

    def upload_local_s3(self, source, partition_path):
        """This method will upload the file into dummy local s3 folder"""
        try:
            dest = os.path.join(self.local_s3_path, partition_path)
            os.makedirs(dest, exist_ok=True)
            shutil.copy(source, dest)
            self.logging.info("File successfully uploaded into dummy S3")
            response = True
            print(partition_path)
        except Exception as err:
            print(err)
            self.logging.error(
                f"Error occured while copy file source to destination due to {err}"
            )
            response = False
        return response
