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
        self.last_run = get_date(config["pull_weather_information_from_metaweather_api"]["last_date"])
        self.path = os.path.join(parent_dir, config["local"]["local_file_path"], "meta_weather")
        os.makedirs(self.path, exist_ok=True)
        self.metaweather = MetaWeatherApi(logger)
        logger.info("Object successfully created!!!")

    def get_weather_information_for_given_dates(self):
        """This method will get the weather information from start date to end date"""
        check_date = datetime.now().date()
        if self.s_date :
            if self.s_date < check_date:
                response = self.get_weather_information_between_two_dates(self.s_date, self.e_date)
                logging.info("weather information took for start date to end date")
            print("Script ran for start and end date. So script was terminated without update last run..")
            sys.exit()
        elif self.last_run and self.last_run < check_date:
            print(self.last_run)
            response =self.get_weather_information_between_two_dates(self.last_run, check_date)
            logging.info("weather information took for last run date to yesterday's date")
        else:
            data_exist = "Data available upto date..."
            self.get_weather_information(check_date-timedelta(1)) if not self.last_run else print(data_exist)
            response = None
            logging.info("weather information took for yesterday's date")
        return response

    def get_weather_information_between_two_dates(self, start, end):
        """This method will get the information between two dates of response"""
        try:
            dates = []
            while start < end:
                self.get_weather_information(start)
                logging.info(f"weather information successfully took for {start}")
                dates.append(start)
                start = start + timedelta(1)
        except Exception as err:
            print(err)
            dates = None
        return dates    

    def get_weather_information(self, date):
        """This method used to get the woeid of city from api"""
        try:
            search_date = date.strftime("%Y/%m/%d")
            for key, value in self.woeid_date.items():
                response = self.metaweather.weather_information_using_woeid_date(value, search_date)
                if response is None:
                    print("Script was terminated due error in api response..")
                    sys.exit()
                self.get_given_date_response(response, key, date)
                # break #--for testcase run 
        except Exception as err :
            response = None
        return response   

    def get_given_date_response(self, response, city, date):
        """This method used to get only required date of information alone"""
        try:
            date = date.strftime("%Y-%m-%d")
            res_df = pd.DataFrame(response)
            for i in range(24):
                t_str = "{date}T{hour}".format(date=date, hour=str(i).zfill(2))
                new_df = res_df[(res_df.created.str.contains(t_str))]
                if not new_df.empty:
                    self.create_json_file(new_df, city, t_str)
            status = True
        except Exception as err:
            print(err)
            status = False
        return status

    def create_json_file(self, new_df, city, t_str):
        """This method is used for create the json file for the weather information
        record as same hour"""
        try :
            epoch = int(time())
            file_name = f"metaweather_{city}_{epoch}.json"
            new_df.to_json(self.path + "/" + file_name, orient="records", lines=True)
            key = self.get_partition_path(city, t_str) + "/" + file_name
            # self.upload_to_s3(self.path + "/" + file_name, key)
            self.upload_to_dummy_s3(self.path + "/" + file_name, self.get_partition_path(city, t_str))
            print(key +" is uploaded")
            json_file_path = self.path+'/'+file_name
        except Exception as err:
            json_file_path =  None
        return json_file_path

    def upload_to_s3(self, file, key):
        """This method used to upload the file to s3 which data got from api"""
        s3_service = S3Service(logger)
        response = s3_service.upload_file(file, key)
        return response

    def upload_to_dummy_s3(self, source, partition_path):
        """This method used to upload the file to dummy s3 which data got from api"""
        dummy_s3 = DummyS3(config, logger)
        response = dummy_s3.upload_dummy_local_s3(source, partition_path)
        return response

    def get_partition_path(self, city, t_str):
        """This method will make key name for uploading into s3 with partition"""
        try:
            date_obj = datetime.strptime(t_str, "%Y-%m-%dT%H")
            partition_path = date_obj.strftime(
                f"pt_city={city}/pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/")
        except Exception as err:
            print(err)
            partition_path = None
        return partition_path


def get_date(datestr):
    """This is the function that returns the date object to the type of s_date argparser argument"""
    try:
        date_obj = datetime.strptime(datestr, "%Y-%m-%d").date()
    except Exception as err:
        print(err)
        return None
    return date_obj


def main():
    """This is the main method for the module api_connection"""
    parser = argparse.ArgumentParser(description="Help to get data from given date")
    parser.add_argument(
        "--s_date",
        type=get_date,
        help="Enter date for pull data",
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
    with open(parent_dir + "/develop.ini", "w", encoding="utf-8") as file:
        config.write(file)


if __name__ == "__main__":
    main()
    set_last_run()
