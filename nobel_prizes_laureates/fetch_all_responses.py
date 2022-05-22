"""This module is used for fetch the nobe prizes, laureates data
from api and upload to s3 with the year wise partition as single file"""

import ast
import os
from datetime import date, datetime
import configparser
import argparse
import sys
import pandas as pd
from nobelprize_api import NobelPrizeApi
from dummy_S3.dummy_s3 import DummyS3
from S3.s3 import S3Service
from logging_and_download_path import LoggingDownloadpath,parent_dir

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
logger_donload = LoggingDownloadpath(config)
logging = logger_donload.set_logger('nobel_prizes_laureates')


class NobelprizeLaureates:
    """This class has the methods for getting api response for nobelprizes and laureates convert
    them into data frame filter the data frame based on the year, convert the filtered
    dataframe as json,and upload to s3 with partition year"""

    def __init__(self, nobel_prize_year, year_to) -> None:
        """This is the init method for the class NobelprizeLaureates"""
        
        self.year = nobel_prize_year
        self.year_to = year_to + 1 if year_to else None
        self.nobelprize_api = NobelPrizeApi(config,logging)
        self.download_path = logger_donload.set_downloadpath("nobelPrize_laureates")
        print(self.download_path)
        self.dummy_s3 = DummyS3(config,logging)

    def get_dataframe_response(self, endpoint, path, data_key):
        """This method will use to get the data as data frame"""
        try:
            if self.year and self.year_to:
                endpoint = f'{endpoint}?nobelPrizeYear={self.year}&yearTo={self.year_to}'
                year_range = f'{self.year}_to_{self.year_to}'
            data_frame = self.nobelprize_api.fetch_all_response(endpoint,data_key)
            if not data_frame.empty:
                name = data_key+year_range if 'year_range' in locals() else data_key
                self.create_json(name, data_frame, path)
                logging.info("%s data fetched for the year %s...", data_key, self.year)
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame

    def create_json(
        self,
        name,
        data_frame,
    ):
        """This method will create the json file from the given dataframe"""
        try:
            file_name = f"{name}.json"
            print(file_name)
            # if data_frame.empty : # for testing
            #     return False
            data_frame.to_json(
                self.download_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            # self.upload_to_s3(self.path + "/" + file_name, key)
            logging.info("Json file Sucessfully created...")
            
        except Exception as err:
            print(err)
            logging.error("Json file not created...")
        return not "err" in locals()

def validate_year(input_year):
    """This method """
    try:
        return datetime.strptime(input_year, "%Y").date().year
    except ValueError:
        msg = f"not a valid year: {input_year!r}"
        raise argparse.ArgumentTypeError(msg)

def main():
    """This the main method for the module fetch_nobelpriz_and_laureates"""
    logging.info("\n\nScript started to execute....")
    today_date = date.today()
    parser = argparse.ArgumentParser(description="Argparser for get input from user")
    parser.add_argument(
        "--nobel_prize_year",
        type=validate_year,
        help="Enter year for fetch data in the format 'YYYY'",
        default=today_date.year - 1,
    )
    parser.add_argument("--year_to", type=validate_year, help="Enter year_to for fetch data in the format 'YYYY'")
    parser.add_argument(
        "endpoint",
        choices=["prize", "laureates"],
        help="Choose from choices for get single endpoint response either prize or laureates",
    )
    args = parser.parse_args()
    nobel = NobelprizeLaureates(args.nobel_prize_year,args.year_to)
    logging.info("Script started to fetch data for the endpoint %s....", args.endpoint)
    kwargs = (
        config["nobel_api"]["prize_arguments"]
        if args.endpoint == "prize"
        else config["nobel_api"]["lauretes_arguments"]
    )
    kwargs = ast.literal_eval(kwargs)
    nobel.get_dataframe_response(**kwargs)


if __name__ == "__main__":
    main()
