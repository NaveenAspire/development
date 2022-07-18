"""This module is used to connect the sap concur api and access the endpoints"""

import sys
import requests
from cryptography.fernet import Fernet


class SapApi:
    """This is the class have methods for each endpoints"""

    def __init__(self, section) -> None:
        """This is the init method for the class SapApi"""
        self.section = section
        fernet_key = self.section.get("fernet_key")
        fernet = Fernet(fernet_key)
        encrypted_key = bytes(self.section.get("user_access_token"), "utf-8")
        access_key = fernet.decrypt(encrypted_key).decode()
        self.header = {"token": access_key, "type": "member"}

    def get_reports(self, params):
        """This method will fetch all reports from SAP concur api based on date"""
        try:
            params["Accept"] = "application/json"
            endpoint = self.section.get("reports_endpoint")
            response = requests.get(endpoint, headers=self.header, params=params)
            if response != 200:
                raise Exception
        except Exception as err:
            print(err)
            sys.exit(f"Error occured due to : {err}")
        return response
