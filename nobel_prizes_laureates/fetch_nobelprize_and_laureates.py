"""This module is used for fetch the nobe prizes, laureates data
from api and upload to s3 with the year wise partition as single file"""

import ast
import os
from datetime import date, datetime
import configparser
import argparse
import logging
import sys
import pandas as pd
from nobelprize_api import NobelPrizeApi
from dummy_S3.dummy_s3 import DummyS3
from S3.s3 import S3Service


parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")

log_dir = os.path.join(parent_dir, config["local"]["local_log"], "nobelprize_laureates")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "nobelprize_laureates.log")

logging.basicConfig(
    filename=log_file,
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger()


class NobelprizeLaureates:
    """This class has the methods for getting api response for nobelprizes and laureates convert
    them into data frame filter the data frame based on the year, convert the filtered
    dataframe as json,and upload to s3 with partition year"""

    def __init__(self, nobel_prize_year, year_to) -> None:
        """This is the init method for the class NobelprizeLaureates"""
        self.year = nobel_prize_year
        self.year_to = year_to + 1 if year_to else None
        self.nobelprize_api = NobelPrizeApi(config, logger)
        self.download_path = os.path.join(
            parent_dir, config["local"]["local_file_path"], "nobelPrize_laureates"
        )
        os.makedirs(self.download_path, exist_ok=True)
        self.dummy_s3 = DummyS3(config, logger)

    def fetch_endpoint_response(self, endpoint, path, data_key):
        """This method will fetch the data depends on the endpoint
        method passed in parameters as dataframe and call the create json method"""
        if self.year_to:
            for spec_year in range(self.year, self.year_to):
                data_frame = self.get_dataframe_response(endpoint, spec_year, path, data_key)
        else:
            data_frame = self.get_dataframe_response(endpoint, self.year, path, data_key)
        # return data_frame if "data_frame" in locals() else False # for testing
        return data_frame if "data_frame" in locals() else sys.exit("You were given wrong range")

    def get_dataframe_response(self, endpoint, year, path, data_key):
        """This method will use to get the data as data frame"""
        try:
            response = self.nobelprize_api.fetch_prize_or_laureaes_response(endpoint, year)
            data_frame = pd.DataFrame.from_records(response.get(data_key))
            if not data_frame.empty:
                self.create_json(data_key, data_frame, year, path)
                logging.info("%s data fetched for the year %s...", data_key, self.year)
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame

    def create_json(
        self,
        name,
        data_frame,
        award_year,
        path,
    ):
        """This method will create the json file from the given dataframe"""
        try:
            file_name = f"{name}_{award_year}.json"
            print(file_name)
            # if data_frame.empty : # for testing
            #     return False
            data_frame.to_json(
                self.download_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            # self.upload_to_s3(self.path + "/" + file_name, key)
            logging.info("Json file Sucessfully created for the year %s", award_year)
            self.dummy_s3.upload_dummy_local_s3(
                os.path.join(self.download_path, file_name),
                path + get_partition(award_year),
            )
        except Exception as err:
            print(err)
            logging.error("Json file not created for the year %s", award_year)
        return not "err" in locals()


def get_partition(award_year):
    """This method will create the partition based on the award year"""
    try:
        partition_path = f"pt_year={award_year}"
    except Exception as err:
        print(err)
        partition_path = None
        logging.error("Json file not created for the year %s", award_year)
    return partition_path

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
    nobel.fetch_endpoint_response(**kwargs)


if __name__ == "__main__":
    main()
