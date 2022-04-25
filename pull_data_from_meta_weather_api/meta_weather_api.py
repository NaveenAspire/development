"""This module that pull data from the api and upload to in s3."""
import re
import shutil
import requests
import configparser
from datetime import datetime, timedelta
import argparse
import pandas as pd
import json
import os
from s3 import S3Service

config = configparser.ConfigParser()
config.read("develop.ini")

class MetaWeatherApi:
    """This class has the methods for each endpoints in the meta weather api"""

    def __init__(self) -> None:
        """This is the init method of the class MeataWeatherApi"""
        pass

    def weather_information_using_woeid_date(self, woeid, date):
        """This method will provide the weather information based on woeid and date
        which give as paramether at endpoint  /api/location/(woeid)/(date)/"""
        endpoint = "https://www.metaweather.com/api/location/{woeid}/{date}/".format(woeid=woeid,date=date)
        response = requests.get(endpoint).json()
        return response


class PullDataFromMetaWeatherApi:
    def __init__(self, s_date,e_date) -> None:
        """This is the init method of the class of PullDataFromApi"""
        self.woeid_date = eval(config["woeid"]["cities_woeid"])
        self.s_date = s_date
        self.e_date = e_date
        self.path = os.path.join(os.getcwd(), config["local"]["local_file_path"])
        self.metaweather = MetaWeatherApi()
        
    def get_weather_information_for_given_dates(self):
        """This method will get the weather information from start date to end date"""
        if self.e_date:
            for n in range(int((self.s_date - self.s_date).days)+2):
                date = self.s_date+timedelta(n)
                print(type(date))
                self.get_weather_information(date)

    def get_weather_information(self,date):
        """This method used to get the woeid of city from api"""
        search_date = date.strftime("%Y/%m/%d")
        for key, value in self.woeid_date.items():
            print(key)
            print(value)
            response = self.metaweather.weather_information_using_woeid_date(
                value, search_date
            )
            self.get_given_date_response(response, key,date)

            return

    def get_given_date_response(self, response, city,date):
        """This method used to get only required date of information alone"""
        date = date.strftime('%Y-%m-%d')
        print(date)
        for information in response:
            if date in information["created"]:
                with open(
                    self.path + city + "_" + information["created"] + ".json", "w"
                ) as file:
                    json.dump(information, file)
                    print("success")
                    return

    def upload_to_s3(self):
        """This method used to upload the file to s3 which data got from api"""
        s3_service = S3Service()
        for file in os.listdir(self.path):
            key = self.get_partition_path(file)+file
            s3_service.upload_file(self.path + file, key)
    
    def upload_dummy_local_s3(self):
        for file in os.listdir(self.path):
            partition_path = self.get_partition_path(file)
            local_s3_path = config["local"]["local_s3_path"]
            dummy_s3 = local_s3_path + partition_path
            print(dummy_s3)
            os.makedirs(dummy_s3,exist_ok=True)
            shutil.copy(self.path+file, dummy_s3)

    def get_partition_path(self, file):
        """This method will make key name for uploading into s3 with partition"""
        pt_values = re.split("_|-|T|:", file)
        partition_path = "pt_city={city_name}/pt_year={year}/pt_month={month}/pt_day={day}/pt_hour={hour}/".format(
            city_name=pt_values[0],
            year=pt_values[1],
            month=pt_values[2],
            day=pt_values[3],
            hour=pt_values[4]
        )
        return partition_path

def get_date(datestr):
    return datetime.strptime(datestr, "%Y-%m-%d").date()


def current_date():
    return datetime.now().date() - timedelta(1)


def main():
    """This is the main method for the module api_connection"""
    parser = argparse.ArgumentParser(description="Help to get data from given date")
    parser.add_argument(
        "--s_date", type=get_date, help="Enter date for pull data", default=datetime.now().date() - timedelta(1)
    )
    parser.add_argument(
        "--e_date", type=get_date, help="Enter date for pull data"
    )
    args = parser.parse_args()
    pull_data = PullDataFromMetaWeatherApi(args.s_date,args.e_date)
    pull_data.get_weather_information_for_given_dates()
    # city_woeid = pull_data.get_weather_information()
    # pull_data.upload_to_s3()
    pull_data.upload_dummy_local_s3()


if __name__ == "__main__":
    main()
