"""This module used to fetch data from public holiday api endpoints
and create the response as json then store json files into
s3 with partition based on date"""

import argparse
import configparser
from datetime import datetime, date
import os
import sys
from logging_and_download_path import LoggingDownloadpath, parent_dir
from public_holiday_api import PublicHoliday
from Temp_s3.temp_s3 import TempS3

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_public_holiday_data")

public_holiday = PublicHoliday(config)


class FetchDataFromPublicHolidayApi:
    """This class for fetching data from public_holiday api and create json
    files for the response then upload those files into s3 with partition based on date"""

    def __init__(self) -> None:
        """This is init method for the class FetchDataFromPublicHolidayApi"""
        self.download_path = logger_download.set_downloadpath(
            "fetch_public_holiday_api_data"
        )
        self.section = config["fetch_public_holiday_data"]

    def get_data_for_given_year_region(self, args):
        """This method will used to get data for given year region"""
        years = (
            list(range(args.get("start"), args.get("end") + 1))
            if args.get("end")
            else [int(date.today().year)]
        )
        print(years)
        if not years:
            sys.exit("The given year or year range is wrong...")
        endpoint_func = (
            self.fetch_public_holidays_data
            if args.get("endpoint") == "public_holidays"
            else self.fetch_long_weekend
        )
        for year in years:
            endpoint_func(year, args.get("code"), args.get("endpoint"))
        return True

    def fetch_public_holidays_data(self, year, country_code, endpoint):
        """This method will get the data about public_holidays details for the given year
        Parameter :
            year : The year of the which need data
            country_code : The code for the country"""
        try:
            data_frame = public_holiday.get_pubilc_holidays(year, country_code)
            if data_frame is None:
                raise ValueError("data frame is None")
            self.create_json_file(data_frame, endpoint, year, country_code)
        except Exception as err:
            error = err
            print(error)
        return not "error" in locals()

    def fetch_next_public_holidays(self, country_code, endpoint):
        """This method will get the data about next_public_holidays details for the given year
        Parameter :
            year : The year of the which need data
            country_code : The code for the country"""
        try:
            data_frame = public_holiday.get_next_public_holidays(country_code)
            if data_frame is None:
                raise ValueError("data frame is None")
            self.create_json_file(
                data_frame, endpoint, datetime.today().date(), country_code
            )
        except Exception as err:
            error = err
            print(error)
        return not "error" in locals()

    def fetch_long_weekend(self, year, country_code, endpoint):
        """This method will get the data about long weekend details for the given year
        Parameter :
            year : The year of the which need data
            country_code : The code for the country"""
        try:

            data_frame = public_holiday.get_long_weekend(year, country_code)
            if data_frame is None:
                raise ValueError("data frame is None")
            self.create_json_file(data_frame, endpoint, year, country_code)
        except Exception as err:
            error = err
            print(error)
        return not "error" in locals()

    def create_json_file(self, data_frame, endpoint, pt_var, country_code):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file
            file_name : The name of the file which going to create"""
        try:
            file_name = (
                f"{endpoint}_{country_code}_{pt_var}.json"
                if endpoint != "next_public_holidays"
                else f"{endpoint}_{country_code}_{datetime.strftime(pt_var, '%Y%m%d')}.json"
            )

            data_frame.to_json(
                os.path.join(self.download_path, file_name),
                orient="records",
                lines=True,
            )
            file_path = os.path.join(self.download_path, file_name)
            logger.info("Json file created successfully as name %s", file_name)
            self.upload_to_s3(file_path, endpoint, pt_var, country_code)
        except Exception as err:
            print(err)
            file_path = None
        return file_path

    def upload_to_s3(self, file_path, endpoint, pt_var, country_code):
        """This method will call the get parition method
        for getting partition path and upload to s3."""
        partition_path = (
            self.get_year_partition(pt_var, country_code)
            if not isinstance(pt_var, date)
            else self.get_date_partition(pt_var, country_code)
        )
        temp = TempS3(config, logger)
        temp.upload_local_s3(
            file_path,
            os.path.join(self.section.get("bucket_path"), endpoint, partition_path),
        )

    def get_year_partition(self, year, country_code):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            partition_path = f"pt_countryCode={country_code}/pt_year={year}/"
        except Exception as err:
            print(err)
            sys.exit(f"Script stopped due to {err}")
        return partition_path

    def get_date_partition(
        self,
        date_var,
        country_code,
    ):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            partition_path = date_var.strftime(
                f"pt_region={country_code}/pt_year=%Y/pt_month=%m/pt_day=%d/"
            )
        except Exception as err:
            print(err)
            sys.exit(f"Script stopped due to {err}")
        return partition_path


def validate_year(input_year):
    """This method will validate the date given by user
    Parameter:
        input_date : The date for validate"""
    try:
        if len(input_year) == 4:
            year = int(input_year)
        else:
            raise ValueError
    except ValueError:
        msg = f"not a valid date: {input_year!r}"
        raise msg from argparse.ArgumentTypeError(msg)
    return year


def validate_region(code):
    """This function will used to validate the region which user given"""
    try:
        region_list = public_holiday.get_available_regions()
        print(region_list)
        if code in region_list:
            valid_code = code
        else:
            raise Exception
    except Exception:
        msg = f"not a valid region: {code!r}"
        raise argparse.ArgumentTypeError(msg)
    return valid_code


def main():
    """This is main function for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get dates fro user for fetching the data from api"
    )
    parser.add_argument(
        "code",
        help="Enter region code for the country",
        type=validate_region,
    )
    parser.add_argument(
        "--start",
        help="Enter year in the following format YYYY",
        type=validate_year,
        default=int(date.today().year),
    )
    parser.add_argument(
        "--end",
        help="Enter year in the following format YYYY",
        type=validate_year,
    )
    parser.add_argument(
        "endpoint",
        choices=["public_holidays", "long_weekend", "next_public_holidays"],
        help="Enter the endpoint from the Choices",
    )
    args = parser.parse_args()
    fetch_data = FetchDataFromPublicHolidayApi()
    fetch_data.fetch_next_public_holidays(
        args.code, args.endpoint
    ) if args.endpoint == "next_public_holidays" else fetch_data.get_data_for_given_year_region(
        args.__dict__
    )


if __name__ == "__main__":
    main()
