"""This module used for fetch data from nobelprize api based on the endpoint called"""
import sys
import requests
import pandas as pd

class NobelPrize_Api:
    """This is the class which contains methods for each endpoint to fetch data"""
    def __init__(self,config_obj,kwargs) -> None:
        """This is the init method for the class NobelPrize_Api"""
        self.nobelprize_endpoint = config_obj["nobel_api"]["nobelprize_endpoint"]
        self.laureates_endpoint = config_obj["nobel_api"]["laureates_endpoint"]
        self.kwargs = kwargs
        pass
    
    def fetch_nobel_prize(self,year):
        """The method used for fetch nobel prize based on the params of endpoint"""
        try:
            params = f'?nobelPrizeYear={year}'
            endpoint = self.nobelprize_endpoint+params
            response = requests.get(endpoint)
            if response.status_code == 200:
                nobel_response = response.json()
            else :
                print(f"Yours response code is {response.status_code}")
                raise Exception
        except Exception as err:
            nobel_response = None
            print(err)
        return nobel_response
            
    def fetch_laureates(self,year):
        """The method used for fetch nobel prize based on the params of endpoint"""
        try:
            params = f'?nobelPrizeYear={year}'
            endpoint = self.laureates_endpoint+params
            response = requests.get(endpoint)
            if response.status_code == 200:
                nobel_response = response.json()
            else :
                print(f"Yours response code is {response.status_code}")
                raise Exception
        except Exception as err:
            nobel_response = None
            print(err)
        return nobel_response