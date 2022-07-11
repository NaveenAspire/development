"""This module has the class for connecting thirukkural api
and has methods for fetch data from endpoints """
import requests
import pandas as pd


class Thirukkural:
    """This class used connect and access api endpoints"""

    def __init__(self, section) -> None:
        """This is the init method for the class of Thirukkural
        parameter :
            config_obj : ConfigParser object
        """
        self.section = section

    def get_thirukkural(self, num):
        """This method will call endpoint for get the thirukkural based on given thirukkural number
        Parameter :
            num : The number of the thirukkurual
        Return :
            response : data frame for the response we got"""
        try:
            endpoint = self.section.get("thirukkural_endpoint")
            response = requests.get(endpoint, params={"num": num})
            if not response.status_code == 200:
                raise ValueError("Invalid request")
            data_frame = pd.DataFrame.from_records([response.json()])
            
        except Exception as err:
            # print(err)
            data_frame = None
        return data_frame
