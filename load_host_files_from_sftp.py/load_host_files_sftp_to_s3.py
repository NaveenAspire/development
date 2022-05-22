"""This module will used to load the aspire host files from
sftp into s3 with partition by day using date in filename"""

from datetime import datetime
import configparser
import logging
import os

from nobel_prizes_laureates.fetch_nobelprize_and_laureates import get_partition
from sftp_connection import SftpCon
from S3.s3 import S3Service
from zipfile import ZipFile
from logging_and_download_path import LoggingDownloadpath,parent_dir

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir,'develop.ini'))

class LoadHostFilesSftpToS3:
    """This the class which contains methods 
    for the load the sftp files into s3"""
    
    def __init__(self) -> None:
        """This is the init method for the class LoadHostFilesSftpToS3""" 
        logger_dwonload = LoggingDownloadpath(config)
        self.logger = logger_dwonload.set_logger(__name__)
        self.zip_download_path = logger_dwonload.set_downloadpath("host_aspire_zip_files")
        self.zip_download_path = logger_dwonload.set_downloadpath("host_aspire_txt_files")
        os.makedirs(self.download_path, exist_ok=True)
        self.sftp_obj = SftpCon(config,self.logger)
        self.s3_obj = S3Service(self.logger)
    
    def download_host_files(self,):
        """This method will download all the host aspire zipfiles from sftp """
        try :
            s3_files = self.s3_obj.get_file_list('source/')
            self.sftp_obj.get_new_file_only(self.zip_download_path,s3_files)
            for file in os.listdir(self.zip_download_path):
                with ZipFile(os.path.join(self.zip_download_path,file), 'r') as zip:
                    zip.extractall(self.txt_download_path)
            self.upload_host_files_to_s3(self.zip_download_path,'source')
            self.upload_host_files_to_s3(self.txt_download_path,'stage')
        except Exception as err :
            print(err)
            response = None
        return response
            
    def upload_host_files_to_s3(self, file_path, bucket_path):
        """This method will upload the both zip files and txt files into appropriate paths"""
        try :
            for file in os.listdir(file_path):
                partition = self.get_partition(file.split('.')[0])
                key = os.path.join(bucket_path,partition,file)
                response = self.s3_obj.upload_file(os.path.join(file_path,file),key)
        except Exception as err :
            print(err)
            response = None
        return response            
            
    def get_partition(self,name):
        """This method will used to get partition path of the file"""
        try :
            date = datetime.strptime(name,'ASP_%Y%m%d').date()
            partition_path = datetime.strftime(date,"pt_year=%Y/pt_month=%m/pt_day=%d/")
        except Exception as err :
            print(err)
            partition_path =  None
        return partition_path    

def main():
    load_host_files = LoadHostFilesSftpToS3()
    load_host_files.download_host_files()
    
if __name__ == "__main__":
    main()
    
