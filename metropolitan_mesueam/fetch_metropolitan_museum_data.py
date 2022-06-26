"""This module used to fetch data from metropolitan museum api endpoints
and create the response as json then store json files into
s3 with partition based on given object id"""

import argparse
import configparser
from datetime import datetime
import os
import sys
from logging_and_download_path import LoggingDownloadpath, parent_dir
from metroploitan_mesuem_api import MetropolitanMesuem
from Temp_s3.temp_s3 import TempS3

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_metropolitan_museum_data")


class FetchMetropolitanMuseumData:
    """This class for fetching data from metropolitan_museum api and create json
    files for the response then upload those files into s3 with partition based on object id"""

    def __init__(self) -> None:
        """This is init method for the class FetchMetropolitanMuseumData"""
        self.download_path = logger_download.set_downloadpath(
            "fetch_metropolitan_museum_data"
        )
        self.metropolitan_museum = MetropolitanMesuem(config)
        self.section = config["fetch_metropolitan_museum_data"]

    def fetch_object_details(self, object_id):
        """This method will used to fetch the object detail of given object id"""
        try:
            data_frame = self.metropolitan_museum.get_object_details(object_id)
            if data_frame is None:
                raise ValueError("data frame is None")
            self.create_json_file(data_frame, object_id)
        except Exception as err:
            error = err
            print(error)
        return not "error" in locals()

    def create_json_file(self, data_frame, object_id):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file
            file_name : The name of the file which going to create"""
        try:
            file_name = f"{object_id}.json"

            data_frame.to_json(
                os.path.join(self.download_path, file_name),
                orient="records",
                lines=True,
            )
            file_path = os.path.join(self.download_path, file_name)
            logger.info("Json file created successfully as name %s", file_name)
            self.upload_to_s3(file_path, object_id)
        except Exception as err:
            print(err)
            file_path = None
        return file_path

    def upload_to_s3(self, file_path, object_id):
        """This method will call the get parition method
        for getting partition path and upload to s3."""
        partition_path = self.get_partition(object_id)
        temp = TempS3(config, logger)
        temp.upload_local_s3(
            file_path, os.path.join(self.section.get("bucket_path"), partition_path)
        )

    def get_partition(self, object_id):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            partition_path = f"pt_id={object_id}"
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
        raise msg from argparse.ArgumentTypeError(msg)


def main():
    """This is main function for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get dates fro user for fetching the data from api"
    )
    parser.add_argument(
        "id",
        help="Enter the object_id ",
        type=int,
    )
    args = parser.parse_args()
    fetch_data = FetchMetropolitanMuseumData()
    fetch_data.fetch_object_details(args.id)


if __name__ == "__main__":
    main()
