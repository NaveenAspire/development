"""This module used to fetch data from thirukkural api endpoints
and create the response as json then store json files into
s3 with partition based on date"""

import argparse
import configparser
import os
import sys
from logging_and_download_path import LoggingDownloadpath, parent_dir
from thirukkural_api import Thirukkural
from Temp_s3.temp_s3 import TempS3

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_ebird_api_data")


class FetchThirukkuralData:
    """This class for fetching data from sunrise_sunset api and create json
    files for the response then upload those files into s3 with partition based on date"""

    def __init__(self) -> None:
        """This is init method for the class FetchDataFromSunriseSunsetApi"""
        self.download_path = logger_download.set_downloadpath("fetch_thirukkural")
        self.section = config["fetch_thirukkural"]

    def get_response_for_given_input(self, args):
        """This method will get the response based on the user input"""
        thirukkural_numbers = (
            list(range(args.s_num, args.e_num + 1))
            if args.s_num and args.e_num
            else args.num
            if args.num
            else sys.exit("Please give thirukkural number...")
        )
        for num in thirukkural_numbers:
            self.fetch_thirukkural(num)

    def fetch_thirukkural(self, num):
        """This method will fetch the thirukkural based on the given thirukural num
        Parameter :
            num : The thirukkural number
        Return : returns True or False"""
        try:
            print(f"Thirukkural - {num}")
            thirukkural = Thirukkural(config)
            data_frame = thirukkural.get_thirukkural(num)
            if data_frame is None:
                raise ValueError("The response is None")
            logger.info("Successfully get the thirukkural %s", num)
            self.create_json_file(data_frame, num)
        except Exception as err:
            logger.error("Error occured while getting thirukkural %s", num)
            print(err)
        return data_frame

    def create_json_file(self, data_frame, num):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file"""
        try:
            file_name = f"thirukkural_{num}.json"
            data_frame.to_json(
                os.path.join(self.download_path, file_name),
                force_ascii=False,
                orient="records",
                lines=True,
            )
            file_path = os.path.join(self.download_path, file_name)
            logger.info("Json file created successfully as name %s", file_name)
            partition_path = self.get_partition(data_frame)
            self.upload_to_s3(file_path, partition_path)
        except Exception as err:
            print(err)
            file_path = None
        return file_path

    def upload_to_s3(self, file_path, partition_path):
        """This method will call the get parition method for
        getting partition path and upload to s3."""
        temp = TempS3(config, logger)
        key = os.path.join(self.section.get("bucket_path"), partition_path)
        temp.upload_local_s3(file_path, key)
        logger.info("File successfully uploaded to s3..")

    def get_partition(self, data_frame):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            partition_path = f"pt_section={data_frame.at[0,'sect_eng']}/pt_chaptergroup={data_frame.at[0,'chapgrp_eng']}/pt_chapter={data_frame.at[0,'chap_eng'].replace(' ','_')}/pt_number={data_frame.at[0,'number']}/"
        except Exception as err:
            print(err)
            sys.exit(f"Script stopped due to {err}")
        return partition_path


def main():
    """This is main function for this module"""
    logger.info("\nScript is started ....")
    parser = argparse.ArgumentParser(
        description="This argparser used for get dates for user for fetching the data from api"
    )
    parser.add_argument(
        "--num", help="Enter thirukkural number which you need", type=int, nargs="+"
    )
    parser.add_argument(
        "--s_num",
        help="Enter start number of thirukkural which you need",
        type=int,
    )
    parser.add_argument(
        "--e_num",
        help="Enter end number of thirukkural which you need",
        type=int,
    )
    args = parser.parse_args()
    fetch_data = FetchThirukkuralData()

    fetch_data.get_response_for_given_input(args)


if __name__ == "__main__":
    main()
