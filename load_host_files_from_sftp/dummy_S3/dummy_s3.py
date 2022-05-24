"""This module is used to test the s3 uploading file in local instead of do it in s3"""
import os
import shutil


class DummyS3:
    """This class have the methods for perform s3 operations in local"""

    def __init__(self, config_obj, logger_obj) -> None:
        """This is the init method of the class of DummyS3"""
        self.config = config_obj
        self.logging = logger_obj
        local_s3_path = self.config["local"]["local_s3_path"]
        self.dummy_s3_path = os.path.join(os.path.dirname(os.getcwd()), local_s3_path)

    def get_file_list(self, prefix):
        """This method used to get the list of files from s3 bucket"""
        try:
            rootdir = self.dummy_s3_path + prefix
            file_list = []
            for (dirpath, dirnames, filenames) in os.walk(rootdir):
                file_list += [os.path.join(dirpath, file) for file in filenames]
            self.logging.info("Successfully got the file list from dummy s3...")
        except Exception as err:
            print(err)
            self.logging.error("Error occured while getting s3 file list %s",err)
            file_list = None
        return file_list

    def upload_dummy_local_s3(self, source, bucket_path):
        """This method will upload the file into dummy local s3 folder"""
        try:
            dest = os.path.join(self.dummy_s3_path, bucket_path)
            os.makedirs(dest, exist_ok=True)
            shutil.copy(source, dest)
            self.logging.info("File successfully uploaded into dummy S3")
            response = True
        except Exception as err:
            print(err)
            self.logging.error(
                f"Error occured while copy file source to destination due to {err}"
            )
            response = False
        return response
