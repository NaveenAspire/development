"""This module used for connecting api with the authentication
and fetch the data based on the endpoint called"""

import sys
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
        self.params = {'token' : self.config['free_sound_api']['access_token'],'page_size' : 100}
    
    def similar_sounds(self, sound_id):
        """
        This method access enpoint for find similar sounds based on given sound id
        Parameter:
            sound_id : The id of the sound
        Returns:
            dataframe : collection of json responses as dataframe.
        """
        endpoint = self.config['free_sound_api']['similar_sound'].replace("<sound_id>",sound_id)
        response = self.pagination(endpoint)    
        return response
            
        
    def user_packs(self, username):
        """This method access the endpoint for find packs which created by the user
        Parameter :
            username : The username of the user
        Returns :
            data_frame : collection json responses as dataframe"""
        
        endpoint = self.config['free_sound_api']['user_packs'].replace("<username>",username)
        response = self.pagination(endpoint)
        return response
        
    def pagination(self, endpoint,params):
        """This method used to paginate the response and get the response as single dataframe
        Parameter :
            json_response : The first response of the endpoint
            
        Return :
            data frame : Collection of json respose as single data frame"""
        try :
            dfs = []
            while True:
                response = requests.get(endpoint,params=params)
                if response.status_code != 200 :
                    raise Exception(f"Your endpoint '{endpoint}' returns status code '{response.status_code}'")
                json_response = response.json()
                data_frame = pd.DataFrame.from_records(json_response.get('results'))
                dfs.append(data_frame)
                endpoint = json_response.get("next")
                if not endpoint:
                    # The next link will not be availabe in the response
                    print("There is no next link ...")
                    break
            data_frame = pd.concat(dfs)
        except Exception as err :
            print(f"Error occured : {err}")
            data_frame = None
        return data_frame