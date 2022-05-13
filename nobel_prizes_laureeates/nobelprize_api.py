"""This module used for fetch data from nobelprize api based on the endpoint called"""
from inspect import trace
import requests

class NobelPrize_Api:
    """This is the class which contains methods for each endpoint to fetch data"""
    def __init__(self,config_obj,kwargs) -> None:
        """This is the init method for the class NobelPrize_Api"""
        self.nobelprize_endpoint = config_obj["nobel_api"]["nobelprize_endpoint"]
        self.laureates_endpoint = config_obj["nobel_api"]["laureates_endpoint"]
        self.kwargs = kwargs
        pass
    
    def fetch_nobel_prize(self):
        """The method used for fetch nobel prize based on the params of endpoint"""
        try:
            params = '?'
            for key,value in self.kwargs.items():
                if value != None:
                    params = f'{params}{key}={value}&'
            # print(params)
            endpoint = self.nobelprize_endpoint+params
            while True:
                response = requests.get(endpoint).json()
                response.get('nobelPrizes')
                next = response.get('links').get('next')
                print(endpoint)
                if not next :
                    break
                endpoint = next
        except Exception as err:
            print(err.args)
        # return response
            
    def fetch_laureates(self):
        """The method used for fetch nobel prize based on the params of endpoint"""
        try:
            params = '?'
            for key,value in self.kwargs.items():
                if value != None:
                    params = f'{params}{key}={value}&'
            endpoint = self.laureates_endpoint+params+'limit=100'
            while True:
                response = requests.get(endpoint).json()
                next = response.get('links').get('next')
                if not next :
                    break
                endpoint = next
        except Exception as err:
            print(err)
        return response