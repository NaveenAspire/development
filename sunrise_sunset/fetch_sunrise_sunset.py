"""This module used to fetch data from sunrise_sunset api endpoints
and create the response as json then store json files into
s3 with partition based on date"""

import argparse
import ast
import configparser
from datetime import datetime, timedelta
import os
import sys
from logging_and_download_path import LoggingDownloadpath, parent_dir
from sunrise_sunset_api import SunriseSunset
from Temp_s3.temp_s3 import TempS3
from geopy.geocoders import Nominatim

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_sunrise_sunset_api_data")


class FetchDataFromSunriseSunsetApi:
    """This class for fetching data from sunrise_sunset api and create json
    files for the response then upload those files into s3 with partition based on date"""

    def __init__(self) -> None:
        """This is init method for the class FetchDataFromSunriseSunsetApi"""
        self.download_path = logger_download.set_downloadpath(
            "fetch_sunreise_sunset_api_data"
        )
        self.section = config["fetch_sunrise_sunset_data"]
        self.exists_cities = ast.literal_eval(self.section.get('cities'))
        
    def add_city_to_config(self,args):
        """This method will add the city to config with latitude and logitude value and fetch data for date"""
        try :
            geolocator = Nominatim(user_agent="geoapiExercises")
            location = geolocator.reverse(f"{args.lat},{args.lng}")
            city = location.raw['address'].get('city')
            if not city == args.city :
                sys.exit("Enter the valid city name")
            city_dict = {'lat':args.lat, 'lng':args.lng}
            if city in self.exists_cities:
                sys.exit("The given city already exists...")
            self.fetch_for_given_dates(args,city,args.lat,args.lng)
            self.exists_cities[city] = city_dict
            update_ini('fetch_sunrise_sunset_data','cities',str(self.exists_cities))
            # print(cities.get('chennai').get('lat'))
            # for city,data in self.exists_cities.items():
                
        except Exception as err :
            print(err)
            sys.exit(f"Script terminated due to {err}")
        return True



    def fetch_for_given_dates(self, args, city, lat, lng):
        """This method will analyis the date input in
        argparse and fetch data for the appropriate date"""
        previous_date = datetime.today().date() - timedelta(1)
        start, end = (
            (args.s_date, args.e_date)
            if args.s_date and args.e_date
            else (args.s_date, args.s_date)
            if args.s_date
            else (previous_date, previous_date)
        )
        if start>end:
            logger.error("The date range is wrong...")
            sys.exit("The date range is wrong...")
        while start <= end :
            print(start)
            self.fetch_sunrise_sunset_data(start, city, lat,lng)
            start=start+timedelta(1)

    def fetch_sunrise_sunset_data(self, date, city, latitude, longitude):
        """This method will get the data about sunrise_sunset details for the given date
        Parameter :
            date : The date of the day which need data"""
        try:
            params = {"lat": latitude, "lng": longitude}
            if not date:
                date = datetime.today().date()
            params["date"] = datetime.strftime(date, "%Y-%m-%d")
            sunrise_sunset = SunriseSunset(config)
            data_frame = sunrise_sunset.get_sunrise_sunset_details(params)
            if data_frame is None:
                raise ValueError("data frame is None")
            logger.info(f"The response is successfully get city {city} for the date {date}")
            self.create_json_file(data_frame, date,city, latitude, longitude)
        except Exception as err:
            logger.error(f"The response not get city {city} for the date {date}")
            error = err
            print(error)
        return not "error" in locals()

    def create_json_file(self, data_frame, date, city, latitude, longitude):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file
            file_name : The name of the file which going to create"""
        try:
            file_name = f"{city}_{date.strftime('%Y%m%d')}.json"
            data_frame.to_json(
                os.path.join(self.download_path, file_name),
                orient="records",
                lines=True,
            )
            file_path = os.path.join(self.download_path, file_name)
            logger.info("Json file created successfully as name %s", file_name)
            self.upload_to_s3(file_path, date, city, latitude, longitude)
        except Exception as err:
            print(err)
            logger.error("Json file is not created for %s's response", date)
            file_path = None
        return file_path

    def upload_to_s3(self, file_path, date, city, lat, lng):
        """This method will call the get parition method
        for getting partition path and upload to s3."""
        partition_path = self.get_partition(date, city, lat, lng)
        temp = TempS3(config, logger)
        temp.upload_local_s3(
            file_path, os.path.join(self.section.get("bucket_path"), partition_path)
        )
        logger.info("File successfully uploaded to s3")

    def get_partition(self, date, city, lat, lng):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            partition_path = date.strftime(
                f"pt_city={city}_/pt_year=%Y/pt_month=%m/pt_day=%d/"
            )
        except Exception as err:
            print(err)
            sys.exit(f"Script stopped due to {err}")
        return partition_path


def validate_date(input_date):
    """This method will validate the date given by user
    Parameter:
        input_date : The date for validate"""
    try:
        date = datetime.strptime(input_date, "%Y-%m-%d").date()
        if date >= datetime.today().date():
            raise ValueError
    except ValueError:
        msg = f"not a valid date: {input_date!r}"
        raise msg from argparse.ArgumentTypeError()
    return date

def validate_lat(lat):
    """This method will validate latitude value given by user"""
    try:
        lat = float(lat)
        if not (lat >= -90 and lat <= 90):
            raise ValueError
    except ValueError:
        msg = f"not a valid latitude: {lat!r}"
        raise msg from argparse.ArgumentTypeError()
    return lat

def validate_lng(lng):
    """This method will validate longitude value given by user"""
    try:
        lng = float(lng)
        if not (lng >= -180 and lng <= 180):
            raise ValueError
    except ValueError:
        msg = f"not a valid latitude: {lng!r}"
        raise msg from argparse.ArgumentTypeError()
    return lng

def update_ini(section, name, value):
    """This method used to update the ini file"""
    config.set(section, name, value)
    with open(parent_dir + "/develop.ini", "w", encoding="utf-8") as file:
        config.write(file)

def main():
    """This is main function for this module"""
    logger.info("Script is started ...")
    parser = argparse.ArgumentParser(
        description="This argparser used for get dates fro user for fetching the data from api"
    )
    subparser = parser.add_subparsers()
    add_city = subparser.add_parser('add_city')
    parser.add_argument(
        "--s_date",
        help="Enter date in the following format YYYY-MM-DD",
        type=validate_date,
    )
    parser.add_argument(
        "--e_date",
        help="Enter date in the following format YYYY-MM-DD",
        type=validate_date,
    )
    add_city.add_argument(
        "--city",
        help="Enter the city ",
        type=str
    )
    add_city.add_argument(
        "lat",
        help="Enter latitude value for the place",
        type=validate_lat,
    )
    add_city.add_argument(
        "lng",
        help="Enter longitude value for the place",
        type=validate_lng,
    )
    args = parser.parse_args()
    fetch_data = FetchDataFromSunriseSunsetApi()
    if args.__dict__.get('city'):
        fetch_data.add_city_to_config(args)
        sys.exit("The given city successfully added..")
    for city,val in fetch_data.exists_cities.items() :
        fetch_data.fetch_for_given_dates(args,city,val.get('lat'),val.get('lng'))
    # fetch_data.fetch_for_given_dates(args)
    # fetch_data.fetch_sunrise_sunset_data(args.date, args.lat, args.lng)


if __name__ == "__main__":
    main()
