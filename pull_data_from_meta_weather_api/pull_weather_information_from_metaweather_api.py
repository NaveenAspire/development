"""This module that pull weather information from the meat weather api
as json file and upload to in s3 based on partition city,year,month,day and hour."""
import ast
import configparser
from datetime import datetime, timedelta
import argparse
import logging
import os
import sys
from time import time

import pandas as pd
from dummy_S3.dummy_s3 import DummyS3
from metaweather_api import MetaWeatherApi
from S3.s3 import S3Service

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
log_dir = os.path.join(parent_dir, "opt/logging/pull_weather_information_from_metaweather_api/")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "weather_information.log")

logging.basicConfig(
    filename=log_file,
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    force=True,
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PullWeatherInformationFromMetaWeatherApi:
    """This is the class have methods for pull weather infromation from the metaweather api"""

    def __init__(self, s_date, e_date) -> None:
        """This is the init method of the class of PullDataFromApi"""
        self.woeid_date = ast.literal_eval(
            config["pull_weather_information_from_metaweather_api"]["cities_woeid"]
        )
        self.s_date = s_date
        self.e_date = e_date
        self.path = os.path.join(parent_dir, config["local"]["local_file_path"], "meta_weather")
        os.makedirs(self.path, exist_ok=True)
        self.metaweather = MetaWeatherApi(logger)
        logger.info("Object successfully created!!!")

    def get_weather_information_for_given_dates(self):
        """This method will get the weather information from start date to end date"""
        last_run = config["pull_weather_information_from_metaweather_api"]["last_date"]
        if last_run and self.s_date == self.e_date and get_date(last_run) < self.s_date:
            last_run = datetime.strptime(last_run, "%Y-%m-%d").date()
            self.s_date = last_run
        while self.s_date <= self.e_date:
            self.get_weather_information(self.s_date)
            self.s_date = self.s_date + timedelta(1)

    def get_weather_information(self, date):
        """This method used to get the woeid of city from api"""
        search_date = date.strftime("%Y/%m/%d")
        for key, value in self.woeid_date.items():
            response = self.metaweather.weather_information_using_woeid_date(value, search_date)
            if response is None:
                sys.exit()
            self.get_given_date_response(response, key, date)
        return response

    def get_given_date_response(self, response, city, date):
        """This method used to get only required date of information alone"""
        date = date.strftime("%Y-%m-%d")
        res_df = pd.DataFrame(response)
        for i in range(24):
            t_str = "{date}T{hour}".format(date=date, hour=str(i).zfill(2))
            new_df = res_df[(res_df.created.str.contains(t_str))]
            if not new_df.empty:
                epoch = int(time())
                file_name = "metaweather_{city}_{epoch}.json".format(city=city, epoch=epoch)
                new_df.to_json(self.path + "/" + file_name, orient="records", lines=True)
                key = self.get_partition_path(city,t_str)+'/'+file_name
                self.upload_to_s3(self.path+'/'+file_name,key)
                # self.upload_to_dummy_s3(
                #     self.path + "/" + file_name, self.get_partition_path(city, t_str)
                # )
        return True

    def upload_to_s3(self, file, key):
        """This method used to upload the file to s3 which data got from api"""
        s3_service = S3Service(logger)
        response = s3_service.upload_file(file, key)
        return response

    def upload_to_dummy_s3(self, source, partition_path):
        """This method used to upload the file to dummy s3 which data got from api"""
        dummy_s3 = DummyS3(config)
        response = dummy_s3.upload_dummy_local_s3(source, partition_path)
        return response

    def get_partition_path(self, city, t_str):
        """This method will make key name for uploading into s3 with partition"""
        try:
            date_obj = datetime.strptime(t_str, "%Y-%m-%dT%H")
            partition_path = date_obj.strftime(
                "pt_city={city_name}/pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/".format(
                    city_name=city
                )
            )
        except Exception as err:
            print(err)
        return partition_path


def get_date(datestr):
    """This is the function that returns the date object to the type of s_date argparser argument"""
    return datetime.strptime(datestr, "%Y-%m-%d").date()


def main():
    """This is the main method for the module api_connection"""
    parser = argparse.ArgumentParser(description="Help to get data from given date")
    parser.add_argument(
        "--s_date",
        type=get_date,
        help="Enter date for pull data",
        default=datetime.now().date() - timedelta(1),
    )
    parser.add_argument(
        "--e_date",
        type=get_date,
        help="Enter date for pull data",
        default=datetime.now().date() - timedelta(1),
    )
    args = parser.parse_args()
    pull_data = PullWeatherInformationFromMetaWeatherApi(args.s_date, args.e_date)
    pull_data.get_weather_information_for_given_dates()


def set_last_run():
    """This function that will set the last run of the script"""
    config.set(
        "pull_weather_information_from_metaweather_api", "last_date", str(datetime.now().date())
    )
    with open(parent_dir + "/develop.ini", "w",encoding='utf-8') as file:
        config.write(file)


if __name__ == "__main__":
    main()
    set_last_run()
    # print("sdfsd")