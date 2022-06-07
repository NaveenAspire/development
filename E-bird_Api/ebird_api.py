"""This module has the class for connecting e-bird api with
authentiacation and has methods for fetch data from endpoints """

import requests
import pandas
from cryptography.fernet import Fernet

class EbirdApi :
    """This class used connect api with authentication and access api endpoints"""
    
    def __init__(self,config_obj) -> None:
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
        print(access_key)
        # self.headers = {
        #     "token": access_key,
        # }