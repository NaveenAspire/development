"""This module used for connecting api with the authentication
and fetch the data based on the endpoint called"""

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
        self.params = {'token' : self.config['free_sound_api']['access_token'],'page' : 3000, 'page_size' : 500}
    
    def similar_sounds(self, sound_id):
        """
        This method access enpoint for find similar sounds based on given sound id
        Parameter:
            sound_id : The id of the sound
        Returns:
            dataframe : collection of json responses as dataframe.
        """
        try :
            endpoint = self.config['free_sound_api']['similar_sound'].replace("<sound_id>",sound_id)
            response = requests.get(endpoint,params=self.params)
            self.pagination(endpoint)
            # if response.status_code != 200 :
            #     raise Exception
            # dfs = []
            # json_response = response.json()
            # while True:
            #     data_frame = pd.DataFrame.from_records(json_response.get('results'))
            #     dfs.append(data_frame)
            #     next = json_response.get("next")
            #     if not next:
            #         print("breaked")
            #         break
            #     json_response = requests.get(next,params=self.params).json()
            #     print(json_response)
            # data_frame = pd.concat(dfs)
        except Exception as err :
            print(f"Yours response code is {response.status_code}")
            print(f"Error occured : {err}")
            data_frame = pd.DataFrame()
        # print(data_frame)
        return #data_frame
            
        
    def user_packs(self, username):
        """This method access the endpoint for find packs which created by the user
        Parameter :
            username : The username of the user
        Returns :
            data_frame : collection json responses as dataframe"""
        
        endpoint = self.config['free_sound_api']['user_packs'].replace("<username>",username)
        response = requests.get(endpoint,params=self.params).json()
        print(response)
        
    def pagination(self, endpoint):
        """This method used to paginate the response and get the response as single dataframe
        Parameter :
            json_response : The first response of the endpoint
            
        Return :
            data frame : Collection of json respose as single data frame"""
        try :
            dfs = []
            while True:
                response = requests.get(endpoint,params=self.params)
                if response.status_code != 200 :
                    raise Exception
                json_response = response.json()
                data_frame = pd.DataFrame.from_records(json_response.get('results'))
                dfs.append(data_frame)
                endpoint = json_response.get("next")
                if not endpoint:
                    print("breaked")
                    break
                print("json_response")
            data_frame = pd.concat(dfs)
        except Exception as err :
            print(f"Error occured : {err}")
            data_frame = pd.DataFrame()
        return data_frame