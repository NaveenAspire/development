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
    
    def get_file_list(self, prefix):
        """This method used to get the list of files from s3 bucket"""
        file_list = []
        prefix = self.bucket_path+prefix
        try :
            paginator = self.s3_obj.get_paginator('list_objects_v2')
            for my_bucket in paginator.paginate(Bucket = self.bucket_name, Prefix = prefix):
                if my_bucket.get('Contents'):
                    for file_obj in my_bucket.get('Contents'):
                        file_list.append(file_obj['Key'].split('/'[-1]))
        except ClientError as err:
            print(err)
        return file_list
