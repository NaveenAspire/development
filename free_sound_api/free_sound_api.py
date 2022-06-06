"""This module used for connecting api with the authentication
and fetch the data based on the endpoint called"""

import traceback
from cryptography.fernet import Fernet
import requests
import pandas as pd


class FreeSoundApi:
    """This class for authenticate and access endpoint of the free sound api"""

    def __init__(self, config_obj) -> None:
        """
        This is the init method for the class of FreeSoundApi

        parameters:
            config : configParser object
        """
        self.config = config_obj
        fernet_key = self.config["free_sound_api"]["fernet_key"]
        fernet = Fernet(fernet_key)
        encrypted_key = bytes(self.config["free_sound_api"]["access_token"], "utf-8")
        access_key = fernet.decrypt(encrypted_key).decode()
        self.params = {
            "token": access_key,
            "page_size": 100,
        }

    def similar_sounds(self, sound_id):
        """
        This method access enpoint for find similar sounds based on given sound id
        Parameter:
            sound_id : The id of the sound
        Returns:
            dataframe : collection of json responses as dataframe.
        """
        endpoint = self.config["free_sound_api"]["similar_sound"].replace(
            "<sound_id>", sound_id
        )
        response = pagination(endpoint, self.params)
        return response

    def user_packs(self, username):
        """This method access the endpoint for find packs which created by the user
        Parameter :
            username : The username of the user
        Returns :
            data_frame : collection json responses as dataframe"""

        endpoint = self.config["free_sound_api"]["user_packs"].replace(
            "<username>", username
        )

        response = pagination(endpoint, self.params)
        return response

def pagination(endpoint, params):
    """This method used to paginate the response and get the response as single dataframe
    Parameter :
        json_response : The first response of the endpoint

    Return :
        data frame : Collection of json respose as single data frame"""
    try:
        dfs = []
        while True:
            response = requests.get(endpoint, params=params)
            print(response)
            if response.status_code != 200:
                raise Exception(
                    f"Your endpoint '{endpoint}' returns status code '{response.status_code}'"
                )
            json_response = response.json()
            results = list(filter(None, json_response.get("results")))
            data_frame = pd.DataFrame.from_records(results)
            dfs.append(data_frame)
            endpoint = json_response.get("next")
            if not endpoint:
                break
        data_frame = pd.concat(dfs)
    except Exception as err:
        print(traceback.format_exc())
        print(f"Error occured : {err}")
        data_frame = None
    return data_frame
