"""This module used to fetch data from ebird api endpoints
and create the response as json then store json files into
s3 with partition based on date"""

import argparse
import configparser
from datetime import datetime
import os
from logging_and_download_path import LoggingDownloadpath, parent_dir
import pandas as pd
from ebird_api import EbirdApi

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir,'develop.ini'))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_ebird_api_data")

class FetchDataFromEbirdApi :
    """This class for fetching data from ebird api and create json
    files for the response then upload those files into s3 with partition based on date"""
    
    def __init__(self) -> None:
        """This is init method for the class FetchDataFromEbirdApi"""
        self.ebird_api = EbirdApi(config)
        self.download_path = logger_download.set_downloadpath("fetch_ebird_api_data")
        self.section = config["fetch_ebird_api_data"]
        
def validate_date():
    pass
        
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
    args = parser.parse_args()
    fetch_data = FetchDataFromEbirdApi()
    
if __name__ == "__main__" :
    main()