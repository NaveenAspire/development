"""This module has the class for connecting metropolitan_mesuem api
and has methods for fetch data from endpoints """

import requests
import pandas as pd


class MetropolitanMesuem:
    """This class used connect and access api endpoints"""

    def __init__(self, config_obj) -> None:
        """This is the init method for the class of MetropolitanMesuem
        parameter :
            config_obj : ConfigParser object
        """
        self.config = config_obj
        self.section = config_obj["metropolitan_museum"]

    def get_object_details(self, object_id):
        """This method will call endpoint for get the object details based on given object id
        Parameter :
            object_id : The object id of the object
        Return :
            response : data frame for the response we got"""
        try:
            endpoint = self.section.get("object_endpoint").replace(
                "[objectID]", str(object_id)
            )
            print(endpoint)
            response = requests.get(endpoint)
            if not response.status_code == 200:
                raise ValueError("Invalid request")
            data_frame = pd.DataFrame.from_records([response.json()])
        except Exception as err:
            print(err)
            data_frame = None
        return data_frame
