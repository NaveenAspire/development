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

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(os.path.join(parent_dir,'develop.ini'))

log_dir =  os.path.join(parent_dir, config["local"]["local_log"], "load_host_files")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "load_host_files.log")

logging.basicConfig(
    filename=log_file,
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger()

class LoadHostFilesSftpToS3:
    """This the class which contains methods 
    for the load the sftp files into s3"""
    
    def __init__(self) -> None:
        """This is the init method for the class LoadHostFilesSftpToS3"""
        self.zip_download_path = os.path.join(
            parent_dir, config["local"]["local_file_path"], "host_aspire_zip_files"
        )
        self.txt_download_path = os.path.join(
            parent_dir, config["local"]["local_file_path"], "host_aspire_txt_files"
        )
        os.makedirs(self.download_path, exist_ok=True)
        self.sftp_obj = SftpCon(config,logger)
        self.s3_obj = S3Service(logger)
    
    def download_host_files(self,):
        """This method will download all the host aspire zipfiles from sftp """
        try :
            response = self.sftp_obj.get_all_files(self.zip_download_path)
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
    
