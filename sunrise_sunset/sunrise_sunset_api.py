"""This module has the class for connecting sunrise_sunser api
and has methods for fetch data from endpoints """

import requests
import pandas as pd


class SunriseSunset:
    """This class used connect and access api endpoints"""

    def __init__(self, config_obj) -> None:
        """This is the init method for the class of SunriseSunset
        parameter :
            config_obj : ConfigParser object
        """
        self.config = config_obj
        self.section = config_obj["sunrise_sunset_api"]

    def get_sunrise_sunset_details(self, params):
        """This method will call endpoint for get the sunrise & sunset based on given date
        Parameter :
            params : The params passed for api endpoint like latitude, longitude and date
        Return :
            response : data frame for the response we got"""
        try:
            endpoint = self.section.get("sunrise_sunset")
            response = requests.get(endpoint, params=params)
            if not response.status_code == 200:
                raise ValueError("Invalid request")
            data_frame = pd.DataFrame.from_records([response.json()])
            print(response.json())
            print(data_frame)
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame
