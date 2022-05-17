"""This module is used for fetch the nobe prizes, laureates data
from api and upload to s3 with the year wise partition as single file"""

import os
from time import time
import configparser
import argparse
import logging
import pandas as pd
from nobelprize_api import NobelPrize_Api
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

    def __init__(self, args) -> None:
        """This is the init method for the class NobelprizeLaureates"""
        self.year = args.nobelPrizeYear
        self.year_to = args.year_to + 1 if args.year_to else None
        self.nobelprize_api = NobelPrize_Api(config)
        self.download_path = os.path.join(
            parent_dir, config["local"]["local_file_path"], "nobelPrize_laureates"
        )
        os.makedirs(self.download_path, exist_ok=True)
        self.dummy_s3 = DummyS3(config, logger)
        self.prize_path = config["nobel_api"]["prize_path"]
        self.laureate_path = config["nobel_api"]["laureate_path"]
        logging.info("Object created sucessfully for class NobelprizeLaureates")

    def fetch_nobelprize_data(self):
        """This method will used to fetch nobel prizes data from the api endpoint as dataframe"""
        if self.year_to:
            for spec_year in range(self.year, self.year_to):
                nobel_response = self.nobelprize_api.fetch_nobel_prize(spec_year)
                data_frame = pd.DataFrame.from_records(nobel_response.get("nobelPrizes"))
                if not data_frame.empty:
                    self.create_json(data_frame, spec_year, self.prize_path)
                    logging.info(f"Nobel prize data fetched for the year {spec_year}...")                
        else:
            nobel_response = self.nobelprize_api.fetch_nobel_prize(self.year)
            data_frame = pd.DataFrame.from_records(nobel_response.get("nobelPrizes"))
            if not data_frame.empty:
                self.create_json(data_frame, self.year, self.prize_path)
                logging.info(f"Nobel prize data fetched for the year {self.year}...")

    def fetch_laureates_data(self):
        """This method will used to fetch laureates data from the api endpoint as dataframe"""
        if self.year_to:
            for spec_year in range(self.year, self.year_to):
                laureats_response = self.nobelprize_api.fetch_laureates(spec_year)
                data_frame = pd.DataFrame.from_records(laureats_response.get("laureates"))
                if not data_frame.empty:
                    self.create_json(data_frame, spec_year, self.laureate_path)
                    logging.info(f"Laureates data fetched for the year {spec_year}...")
        else:
            laureats_response = self.nobelprize_api.fetch_laureates(self.year)
            data_frame = pd.DataFrame.from_records(laureats_response.get("laureates"))
            if not data_frame.empty:
                self.create_json(data_frame, self.year, self.laureate_path)
                logging.info(f"Laureates data fetched for the year {self.year}...")

    def create_json(self, data_frame, award_year, path):
        """This method will create the json file from the given dataframe"""
        try:
            epoch = int(time())
            file_name = f"{epoch}.json"
            data_frame.to_json(self.download_path + "/" + file_name, orient="records", lines=True)
            # self.upload_to_s3(self.path + "/" + file_name, key)
            self.dummy_s3.upload_dummy_local_s3(
                self.download_path + f"/{epoch}.json",
                path + self.get_partition(award_year),
            )
            json_file_path = self.download_path + "/" + file_name
            logging.info(f"Json file Sucessfully created for the year {award_year}")
        except (Exception,ValueError) as err:
            print(err)
            logging.error(f"Json file not created for the year {award_year}")
            json_file_path = None
        return json_file_path

    def get_partition(self, award_year):
        """This method will create the partition based on the award year"""
        try:
            partition_path = f"pt_year={award_year}"
        except Exception as err:
            print(err)
            partition_path = None
        return partition_path


def main():
    """This the main method for the module fetch_nobelpriz_and_laureates"""
    parser = argparse.ArgumentParser(description="Argparser for get input from user")
    parser.add_argument(
        "--nobelPrizeYear", type=int, help="Enter year for fetch data", default=2021
    )
    parser.add_argument("--year_to", type=int, help="Enter year_to for fetch data")
    args = parser.parse_args()
    nobel = NobelprizeLaureates(args)
    nobel.fetch_nobelprize_data()
    nobel.fetch_laureates_data()


if __name__ == "__main__":
    main()
