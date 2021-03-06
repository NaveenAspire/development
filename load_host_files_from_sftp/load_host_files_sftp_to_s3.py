"""This module will used to load the aspire host files from
sftp into s3 with partition by day using date in filename"""

from datetime import datetime
import configparser
import os
import shutil
import sys
from zipfile import ZipFile
from logging_and_download_path import LoggingDownloadpath, parent_dir
from S3.s3 import S3Service
from sftp_connection import SftpCon

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("load_host_aspire_files")


class LoadHostFilesSftpToS3:
    """This the class which contains methods
    for the load the sftp files into s3"""

    def __init__(self):
        """This is the init method for the class LoadHostFilesSftpToS3"""

        self.zip_download_path = logger_download.set_downloadpath(
            "host_aspire_zip_files"
        )
        self.txt_download_path = logger_download.set_downloadpath(
            "host_aspire_txt_files"
        )
        self.bucket_source_path = config["load_host_files"]["bucket_source_path"]
        self.bucket_stage_path = config["load_host_files"]["bucket_stage_path"]
        self.rpath = config["load_host_files"]["sftp_rpath"]
        self.sftp_obj = SftpCon(config, logger)
        self.s3_obj = S3Service(logger)

    def download_host_files(
        self,
    ):
        """This method will download all the host aspire zipfiles from sftp"""
        try:
            s3_files = self.s3_obj.get_file_list("source/")
            s3_files = [file.split("/")[-1] for file in s3_files]
            self.get_new_sftp_files(s3_files)
            for file in os.listdir(self.zip_download_path):
                with ZipFile(
                    os.path.join(self.zip_download_path, file), "r"
                ) as zip_file:
                    zip_file.extractall(self.txt_download_path)
            logger.info("zip files Sucessfully downloaded to local")
            self.upload_host_files_to_s3(
                self.zip_download_path, self.bucket_source_path
            )
            logger.info("zip files Sucessfully uploaded to s3")
            self.upload_host_files_to_s3(self.txt_download_path, self.bucket_stage_path)
            logger.info("txt files Sucessfully uploaded to s3")
            shutil.rmtree(self.zip_download_path)
            shutil.rmtree(self.txt_download_path)
        except Exception as err:
            print(err)
            logger.info("error occured such as %s", err)
            response = False
        return response

    def get_new_sftp_files(self, s3_files):
        """This method will download the files from sftp which are not in s3"""
        try:
            sftp_files = self.sftp_obj.list_files(self.rpath)
            new_files = list(set(sftp_files) - set(s3_files))
            if not new_files:
                sys.exit("There are no new files. All are upto date...")
            print(new_files)
            for file in new_files:
                self.sftp_obj.get_file(
                    os.path.join(self.rpath, file), self.zip_download_path
                )
        except Exception as err:
            print(err)
            error = err
            logger.error("Error occured while getting sftp files such as : %s", err)
        return not "error" in locals()

    def upload_host_files_to_s3(self, file_path, bucket_path):
        """This method will upload the both zip files and txt files into appropriate paths"""
        try:
            for file in os.listdir(file_path):
                partition = get_partition(file.split(".")[0])
                key = os.path.join(bucket_path, partition)
                response = self.s3_obj.upload_file(os.path.join(file_path, file), key)
            response = True
        except Exception as err:
            print(err)
            logger.info("error occured such as %s", err)
            response = None
        return response


def get_partition(name):
    """This method will used to get partition path of the file"""
    try:
        date = datetime.strptime(name, "ASP_%Y%m%d").date()
        partition_path = datetime.strftime(date, "pt_year=%Y/pt_month=%m/pt_day=%d/")
    except Exception as err:
        print(err)
        logger.info("error occured such as %s", err)
        partition_path = None
    return partition_path


def main():
    """THis is the main method for the class LoadHostFilesSftpToS3"""
    load_host_files = LoadHostFilesSftpToS3()
    load_host_files.download_host_files()


if __name__ == "__main__":
    logger.info("\n\nScript Load Host Aspire files Started to execute...")
    main()
