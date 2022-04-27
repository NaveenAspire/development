"""This module has s3 service connection and s3 operations"""
import configparser
import boto3
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read("develop.ini")


class S3Service:
    """This class has the methods for s3 service"""

    def __init__(self):
        """This is the init method of the class S3Service"""
        self.s3_obj = boto3.client(
            "s3",
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        )
        self.bucket_name = config["s3"]["bucket_name"]
        self.bucket_path = config["s3"]["bucket_path"]
        self.local_path = config["local"]["local_file_path"]

    def upload_file(self, file, key):
        """This method is used to upload the file into s3 bucket"""
        try:
            key = self.bucket_path + key
            self.s3_obj.upload_file(
                self.local_path + file,
                self.bucket_name,
                key
            )
        except (Exception, ClientError) as error:
            print(error)
            key = None
        return key
