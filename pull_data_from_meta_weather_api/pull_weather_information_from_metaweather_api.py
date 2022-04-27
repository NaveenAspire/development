"""This module that pull weather information from the meat weather api 
as json file and upload to in s3 based on partition city,year,month,day and hour."""
import ast
import re
import configparser
from datetime import datetime, timedelta
import argparse
import json
import logging
import os
import sys
# from dummy_S3.dummy_s3 import DummyS3
from metaweather_api import MetaWeatherApi
from S3.s3 import S3Service

parent_dir = os.path.dirname(os.getcwd())
print(parent_dir)
config = configparser.ConfigParser()
config.read(parent_dir+"/develop.ini")
log_dir = os.path.join(parent_dir,
                        'opt/logging/pull_weather_information_from_metaweather_api/')
print(log_dir)
os.makedirs(log_dir,exist_ok=True)
log_file = os.path.join(log_dir,'weather_information.log')

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
        self.path = os.path.join(os.getcwd(), config["local"]["local_file_path"])
        self.metaweather = MetaWeatherApi()
        logger.info("Object successfully created!!!")

    def get_weather_information_for_given_dates(self):
        """This method will get the weather information from start date to end date"""
        if self.e_date:
            for n in range(int((self.e_date - self.s_date).days) + 1):
                date = self.s_date + timedelta(n)
                print(date)
                self.get_weather_information(date)
        else:
            self.get_weather_information(self.s_date)

    def get_weather_information(self, date):
        """This method used to get the woeid of city from api"""
        search_date = date.strftime("%Y/%m/%d")
        for key, value in self.woeid_date.items():
            response = self.metaweather.weather_information_using_woeid_date(value, search_date)
            if response == None:
                sys.exit()
            self.get_given_date_response(response, key, date)
        return response

    def get_given_date_response(self, response, city, date):
        """This method used to get only required date of information alone"""
        date = date.strftime("%Y-%m-%d")
        for information in response:
            if date in information["created"]:
                with open(self.path + city + "_" + information["created"] + ".json", "w") as file:
                    json.dump(information, file)
                    print("successfully created json file in local")
        return True

    def upload_to_s3(self):
        """This method used to upload the file to s3 which data got from api"""
        s3_service = S3Service()
        for file in os.listdir(self.path):
            key = self.get_partition_path(file) + file
            response = s3_service.upload_file(self.path + file, key)
        return response

    def get_partition_path(self, file):
        """This method will make key name for uploading into s3 with partition"""
        pt_values = re.split("_|-|T|:", file)
        partition_path = "pt_city={city_name}/pt_year={year}/pt_month={month}/pt_day={day}/pt_hour={hour}/".format(
            city_name=pt_values[0],
            year=pt_values[1],
            month=pt_values[2],
            day=pt_values[3],
            hour=pt_values[4],
        )
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
    )
    args = parser.parse_args()
    print(type(args.s_date))
    args.s_date = "fsfs"
    print(args.s_date)
    # pull_data = PullWeatherInformationFromMetaWeatherApi(args.s_date, args.e_date)
    # pull_data.get_weather_information_for_given_dates()
    # pull_data.upload_to_s3()
    
    # dummy_s3 = DummyS3()
    # dummy_s3.upload_dummy_local_s3()

def set_last_run():
    """This function that will set the last run of the script"""
    config.set('pull_weather_information_from_metaweather_api','last_date',str(datetime.now().date()))
    print(config.get("pull_weather_information_from_metaweather_api","last_date"))
    with open(parent_dir+'develop.ini', 'w') as file:
        config.write(file)
        
    
if __name__ == "__main__":
    main()
    # set_last_run()
    # print("sdfsd")
