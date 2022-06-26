"""This module used to fetch data from sunrise_sunset api endpoints
and create the response as json then store json files into
s3 with partition based on date"""

import argparse
import configparser
from datetime import datetime
import os
import sys
from logging_and_download_path import LoggingDownloadpath, parent_dir
from sunrise_sunset_api import SunriseSunset
from Temp_s3.temp_s3 import TempS3

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_sunrise_sunset_api_data")


class FetchDataFromSunriseSunsetApi:
    """This class for fetching data from sunrise_sunset api and create json
    files for the response then upload those files into s3 with partition based on date"""

    def __init__(self) -> None:
        """This is init method for the class FetchDataFromSunriseSunsetApi"""
        self.download_path = logger_download.set_downloadpath(
            "fetch_sunreise_sunset_api_data"
        )
        self.section = config["fetch_sunrise_sunset_data"]

    def fetch_sunrise_sunset_data(self, date, latitude, langitude):
        """This method will get the data about sunrise_sunset details for the given date
        Parameter :
            date : The date of the day which need data"""
        try:
            params = {"lat": latitude, "lng": langitude}
            if not date:
                date = datetime.today().date()
                params["date"] = datetime.strftime(date, "%Y-%m-%d")
            sunrise_sunset = SunriseSunset(config)
            data_frame = sunrise_sunset.get_sunrise_sunset_details(params)
            if data_frame is None:
                raise ValueError("data frame is None")
            self.create_json_file(data_frame, date, latitude, langitude)
        except Exception as err:
            error = err
            print(error)
        return not "error" in locals()

    def create_json_file(self, data_frame, date, latitude, langitude):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file
            file_name : The name of the file which going to create"""
        try:
            file_name = f"lat_{latitude}_lng_{langitude}.json"
            data_frame.to_json(
                os.path.join(self.download_path, file_name),
                orient="records",
                lines=True,
            )
            file_path = os.path.join(self.download_path, file_name)
            logger.info("Json file created successfully as name %s", file_name)
            self.upload_to_s3(file_path, date)
        except Exception as err:
            print(err)
            file_path = None
        return file_path

    def upload_to_s3(self, file_path, date):
        """This method will call the get parition method
        for getting partition path and upload to s3."""
        partition_path = self.get_partition(date)
        temp = TempS3(config, logger)
        temp.upload_local_s3(
            file_path, os.path.join(self.section.get("bucket_path"), partition_path)
        )

    def get_partition(self, date):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            partition_path = date.strftime("pt_year=%Y/pt_month=%m/pt_day=%d/")
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
        raise msg from argparse.ArgumentTypeError()


def main():
    """This is main function for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get dates fro user for fetching the data from api"
    )
    parser.add_argument(
        "--date",
        help="Enter date in the following format YYYY-MM-DD",
        type=validate_date,
    )
    parser.add_argument(
        "lat",
        help="Enter latitude value for the place",
        type=float,
    )
    parser.add_argument(
        "lng",
        help="Enter langitude value for the place",
        type=float,
    )
    args = parser.parse_args()
    fetch_data = FetchDataFromSunriseSunsetApi()
    fetch_data.fetch_sunrise_sunset_data(args.date, args.lat, args.lng)


if __name__ == "__main__":
    main()
