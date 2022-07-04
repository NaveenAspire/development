"""This module has the class for connecting public_holiday api
and has methods for fetch data from endpoints """

import os
import requests
import pandas as pd


class PublicHoliday:
    """This class used connect and access api endpoints"""

    def __init__(self, config_obj) -> None:
        """This is the init method for the class of PublicHoliday
        parameter :
            config_obj : ConfigParser object
        """
        self.config = config_obj
        self.section = config_obj["public_holiday_api"]

    def get_long_weekend(self, year, country_code):
        """This method will call endpoint for get the sunrise & sunset based on given date
        Parameter :
            params : The params passed for api endpoint like latitude, longitude and date
        Return :
            response : data frame for the response we got"""
        endpoint = os.path.join(
            self.section.get("long_weekend_endpoint"), str(year), country_code
        )
        data_frame = self.get_response(endpoint)
        return data_frame

    def get_pubilc_holidays(self, year, country_code):
        """This method will call endpoint for get the sunrise & sunset based on given date
        Parameter :
            params : The params passed for api endpoint like latitude, longitude and date
        Return :
            response : data frame for the response we got"""
        endpoint = os.path.join(
            self.section.get("public_holidays"), str(year), country_code
        )
        data_frame = self.get_response(endpoint)
        return data_frame

    def get_next_public_holidays(self, country_code):
        """This method will call endpoint for get the sunrise & sunset based on given date
        Parameter :
            params : The params passed for api endpoint like latitude, longitude and date
        Return :
            response : data frame for the response we got"""
        endpoint = os.path.join(self.section.get("next_public_holidays"), country_code)
        data_frame = self.get_response(endpoint)
        return data_frame

    def get_available_regions(self):
        """This method used to get the available regions from the endpoint"""
        try:
            endpoint = self.section.get("available_regions")
            response = requests.get(endpoint)
            if not response.status_code == 200:
                raise ValueError("Invalid request")
            region_list = [region["countryCode"] for region in response.json()]
        except Exception as err:
            print(err)
            region_list = None
        return region_list

    def get_response(self, endpoint_with_params):
        """This method will used to get the response from the endpoint as data frame"""
        try:
            response = requests.get(endpoint_with_params)
            if not response.status_code == 200:
                raise ValueError("Invalid request")
            data_frame = pd.DataFrame.from_records([response.json()])
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame
