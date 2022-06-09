"""This module used to fetch data from ebird api endpoints
and create the response as json then store json files into
s3 with partition based on date"""

import argparse
import ast
import configparser
from datetime import datetime, timedelta
import os
import sys
from logging_and_download_path import LoggingDownloadpath, parent_dir
import pandas as pd
from ebird_api import EbirdApi
from Temp_s3.temp_s3 import TempS3

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_ebird_api_data")

current_date = datetime.now().date()


class FetchDataFromEbirdApi:
    """This class for fetching data from ebird api and create json
    files for the response then upload those files into s3 with partition based on date"""

    def __init__(self, endpoint) -> None:
        """This is init method for the class FetchDataFromEbirdApi"""
        self.ebird_api = EbirdApi(config)
        self.download_path = logger_download.set_downloadpath("fetch_ebird_api_data")
        self.section = config["fetch_ebird_api_data"]
        self.endpoint = endpoint
        self.region = ast.literal_eval(self.section.get("region_list"))

    def fetch_data_for_given_dates(self, s_date, e_date):
        last_run = (
            datetime.strptime(self.section.get("last_run"), "%Y-%m-%d")
            if self.section.get("last_run")
            else None
        )
        self.fetch_data_between_dates(
            s_date, e_date
        ) if s_date and e_date else self.fetch_data_between_dates(
            s_date, current_date
        ) if s_date and s_date <= current_date else self.fetch_data_between_dates(
            last_run, current_date
        ) if last_run else self.fetch_data_between_dates(
            self.section.get("initial_run"), current_date
        ) if not s_date and not last_run else sys.exit(
            "You were given Wrong input..."
        )

        # if s_date and e_date:
        #     self.fetch_data_between_dates(s_date, e_date)
        # elif last_run and s_date < last_run:
        #     self.fetch_data_between_dates(s_date, last_run)
        # elif s_date and s_date < current_date:
        #     self.fetch_data_between_dates(s_date, current_date)
        # else:
        #     self.get_response_for_date(last_run)

    def fetch_data_between_dates(self, start, end):
        if start > end:
            sys.exit("The given date range were wrong...")
        while start <= end:
            for region in self.region:
                self.get_response_for_date(start, region)
            start = start + timedelta(1)

    def get_response_for_date(self, date, region):
        # dfs = [self.ebird_api.get_historic_observations(region, date),
        # self.ebird_api.get_top100_contributors(region, date),
        # self.ebird_api.get_checklist_feed(region, date)]
        # for i in dfs :
        #     self.create_json_file(i,date)
        date = datetime.strftime(date, "%Y/%m/%d")

        if self.endpoint == "historic_observations":
            data_frame = self.ebird_api.get_historic_observations(region, date)
            self.create_json_file(data_frame, date, region)
        elif self.endpoint == "top100_contributors":
            self.ebird_api.get_top100_contributors(region, date)
            self.create_json_file(data_frame, date, region)
        else:
            self.ebird_api.get_checklist_feed(region, date)
            self.create_json_file(data_frame, date, region)

    def add_region(self, region):
        try:
            start = datetime.strptime(self.section.get("initial_run"), "%Y-%m-%d").date()
            while start <= current_date:
                self.get_response_for_date(start, region)
                start = start + timedelta(1)
            region_list = ast.literal_eval(self.section.get("region_list"))
            region_list.append(region)
            update_ini("fetch_ebird_api_data", "region_list", region_list)
        except Exception as err:
            print(err)
            print("This is not a valid region...")

    def create_json_file(self, data_frame, date, region):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file
            file_name : The name of the file which going to create"""
        try:
            file_name = f"{self.endpoint}_{date.replace('/','')}.json"
            data_frame.to_json(
                os.path.join(self.download_path, file_name),
                orient="records",
                lines=True,
            )
            file_path = os.path.join(self.download_path, file_name)
            logger.info("Json file created successfully as name %s", file_name)
            self.upload_to_s3(file_path, date, region)
        except Exception as err:
            print(err)
            file_path = None
        return file_path

    def upload_to_s3(self, file_path, date, region):
        """This method will call the get parition method for getting partition path and upload to s3."""
        partition_path = self.get_partition(region, date)
        temp = TempS3(config, logger)
        temp.upload_local_s3(file_path, partition_path)

    def get_partition(self, region, date):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            date_obj = datetime.strptime(date, "%Y/%m/%d")
            partition_path = date_obj.strftime(
                f"{self.endpoint}/pt_region={region}/pt_year=%Y/pt_month=%m/pt_day=%d/"
            )
        except Exception as err:
            print(err)
            sys.exit(f"Script stopped due to {err}")
        return partition_path


def validate_date(input_date):
    """This method will validate the date given by user
    Parameter:
        input_date : The date for validate"""
    try:
        return datetime.strptime(input_date, "%Y-%m-%d").date()
    except ValueError:
        msg = f"not a valid date: {input_date!r}"
        raise argparse.ArgumentTypeError(msg)


def update_ini(section, name, value):
    config.set(section, name, value)
    with open(parent_dir + "/develop.ini", "w", encoding="utf-8") as file:
        config.write(file)


def main():
    """This is main function for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get dates fro user for fetching the data from api"
    )
    parser.add_argument(
        "--s_date",
        help="Enter date in the following format YYYY-MM-DD",
        type=validate_date,
        default=datetime.now().date(),
    )
    parser.add_argument(
        "--e_date",
        help="Enter date in the following format YYYY-MM-DD",
        type=validate_date,
    )
    parser.add_argument(
        "--add_region",
        help="Enter regeion in the following format YYYY-MM-DD",
        type=str
    )
    parser.add_argument(
        "endpoint",
        help="Enter the endpoint to get response",
        choices=["historic_observations", "top100_contributors", "checklist_feed"],
        type=str,
    )
    args = parser.parse_args()
    fetch_data = FetchDataFromEbirdApi(args.endpoint)
    if args.add_region:
        fetch_data.add_region(args.add_region)
    fetch_data.fetch_data_for_given_dates(args.s_date, args.e_date)


if __name__ == "__main__":
    main()
    update_ini(
        "fetch_ebird_api_data", "last_run", datetime.strftime(current_date, "%Y-%m-%d")
    )
