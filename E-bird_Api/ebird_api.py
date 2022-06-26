"""This module has the class for connecting e-bird api with
authentiacation and has methods for fetch data from endpoints """
import requests
import pandas as pd
from cryptography.fernet import Fernet


class EbirdApi:
    """This class used connect api with authentication and access api endpoints"""

    def __init__(self, config_obj) -> None:
        """This is the init method for the class of EbirdApi
        parameter :
            config_obj : ConfigParser object
        """
        self.config = config_obj
        self.section = config_obj["ebird_api"]
        fernet_key = self.section.get("fernet_key")
        fernet = Fernet(fernet_key)
        encrypted_key = bytes(self.section.get("access_token"), "utf-8")
        access_key = fernet.decrypt(encrypted_key).decode()
        self.headers = {
            "X-eBirdApiToken": access_key,
        }

    def get_historic_observations(self, region_code, date):
        """This method will call endpoint for get the historic observations based on given date
        Parameter :
            regieon_code : The regieon code of the regieon for getting historic data
            date : The date for the getting historic data
        Return :
            response : data frame for the response we got"""
        try:
            endpoint = (
                self.section.get("historic_observations")
                .replace("<regionCode>", region_code)
                .replace("<date>", date)
            )
            data_frame = self.get_response(endpoint)
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame

    def get_top100_contributors(self, region_code, date):
        """This method will call endpoint for get the top 100 contributors based on given date
        Parameter :
            regieon_code : The regieon code of the regieon for getting top 100 contributors data
            date : The date for the top 100 contributors data
        Return :
            response : data frame for the response we got"""
        try:
            endpoint = (
                self.section.get("top_100")
                .replace("<regionCode>", region_code)
                .replace("<date>", date)
            )
            data_frame = self.get_response(endpoint)
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame

    def get_checklist_feed(self, region_code, date):
        """This method will call endpoint for get the regional_statistics based on given date
        Parameter :
            regieon_code : The regieon code of the regieon for getting regional_statistics data
            date : The date for the regional_statistics data
        Return :
            response : data frame for the response we got"""
        try:
            endpoint = (
                self.section.get("checklist_feed")
                .replace("<regionCode>", region_code)
                .replace("<date>", date)
            )
            data_frame = self.get_response(endpoint)
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame

    def get_response(self, endpoint):
        """This method will get response for the given
        enpoint and retuns the data frame for that response
        Parameter :
            endpoint : The enpoint for getting the response
        Return :
            data_Frame : This will be dataframe if response 200 or else None"""
        try:
            response = requests.get(endpoint, headers=self.headers)
            if not response.status_code == 200:
                raise ValueError("Invalid request")
            data_frame = pd.DataFrame.from_records(response.json())
        except Exception as err:
            print(err)
            data_frame = None
        # print(data_frame)
        return data_frame
