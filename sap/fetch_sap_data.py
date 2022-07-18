"""This module is used to fetch the sap concur reports data from
 the sap concur api and upload the response as json file into s3"""

import argparse
from datetime import datetime
import os
import pandas as pd
import configparser
import sys
from logging_and_download_path import LoggingDownloadpath, parent_dir
from S3.s3 import S3Service
from sap.sap_api import SapApi

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "msg_develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("slack_data")


class FetchSapData:
    """This class is used to fetch the reports from the
    sap api and upload the response as json file into s3"""

    def __init__(self) -> None:
        """This is the init method for the class FetchSapData"""
        self.section = config["sap_api"]
        self.sap = SapApi(self.section)
        self.download_path = logger_download.set_downloadpath("fetch_sap_data")
        self.s3 = S3Service(logger)

    def fetch_reports_data(self, date):
        """This method will used fetch the analystic data from the slack api"""
        try:
            params = {"modifiedDateAfter": date}
            response = self.sap.get_reports(params)
        except Exception as err:
            print(err)
        return response

    def create_json(self, response, date):
        """This method will create the json file from the given data"""
        try:
            file_name = f'reports_{datetime.strftime(date,"%Y-%m-%m")}'
            file_path = os.path.join(self.download_path, file_name)
            response.to_json(
                file_path,
                orient="records",
                lines=True,
            )
            self.upload_to_s3(file_path, date)
        except Exception as err:
            print(err)
            sys.exit("The Error occured during decompress the json file")
        return file_path

    def upload_to_s3(self, file_path, date):
        """This method will upload the file into the AWS s3"""
        try:
            partition = self.get_partition(date)
            file_name = file_path.split("/")[-1]
            key = os.path.join(self.section.get('bucket_path'),partition, file_name)
            self.s3.upload_file(file_path, key)
        except Exception as err:
            print(err)
        return True

    def get_partition(self, date):
        """This method used to get the partition based on date"""
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            date_obj = datetime.strptime(date, "%Y/%m/%d")
            partition_path = date_obj.strftime("pt_year=%Y/pt_month=%m/pt_day=%d/")
        except Exception as err:
            sys.exit(f"Script stopped due to {err}")
        return partition_path


def validate_date(input_date):
    """This method will validate the date given by user
    Parameter:
        input_date : The date for validate"""
    try:
        input_date = datetime.strptime(input_date, "%Y-%m-%d").date()
        if input_date < datetime.strptime("2020-01-01", "%Y-%m-%d"):
            raise ValueError
    except ValueError:
        msg = f"not a valid date: {input_date!r}"
        raise argparse.ArgumentTypeError(msg)
    return input_date


def main():
    """This is the main method for this module"""
    fetch_data = FetchSapData()
    parser = argparse.ArgumentParser()
    parser.add_argument("--s_date", type=validate_date, default=datetime.today().date())
    parser.add_argument(
        "--e_date",
        type=validate_date,
    )
    args = parser.parse_args()
