"""This module has s3 service connection and s3 operations"""
import configparser
import os
import boto3
from botocore.exceptions import ClientError

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir+"/develop.ini")


class S3Service:
    """This class has the methods for s3 service"""

    def __init__(self,logger_obj):
        """This is the init method of the class S3Service"""
        self.logging = logger_obj
        self.s3_obj = boto3.client(
            "s3",
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        )
        self.logging.info("Successfully s3 connection made..")
        self.bucket_name = config["s3"]["bucket_name"]
        self.bucket_path = config["s3"]["bucket_path"]
        self.local_path = config["local"]["local_file_path"]
        print(self.s3_obj)
        self.logging.info("Object created Successfully for S3Service class")

    def upload_file(self, file, key):
        """This method is used to upload the file into s3 bucket"""
        try:
            key = self.bucket_path + key
            self.s3_obj.upload_file(file,
                self.bucket_name,
                key
            )
            self.logging.info(file+" sucessfully uploaded into s3")
        except (Exception, ClientError) as error:
            print(error)
            key = None
        return key
