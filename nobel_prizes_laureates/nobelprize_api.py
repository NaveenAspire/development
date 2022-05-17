"""This module used for fetch data from nobelprize api based on the endpoint called"""
import sys
import requests
from datetime import date


class NobelPrizeApi:
    """This is the class which contains methods for each endpoint to fetch data"""

    def __init__(self, config_obj) -> None:
        """This is the init method for the class NobelPrize_Api"""
        self.nobelprize_endpoint = config_obj["nobel_api"]["nobelprize_endpoint"]
        self.laureates_endpoint = config_obj["nobel_api"]["laureates_endpoint"]
        today_date = date.today()
        self.min_year = 1901
        self.max_year= today_date.year if today_date.month >=12 and today_date.day >10 else today_date.year -1
        

    def fetch_nobel_prize(self, year):
        """The method used for fetch nobel prize based on the params of endpoint"""
        try:
            if year not in range(self.min_year,self.max_year+1):
                sys.exit(f"{year} is Not a valid year")
            params = f"?nobelPrizeYear={year}"
            endpoint = self.nobelprize_endpoint + params
            response = requests.get(endpoint)
            if response.status_code == 200:
                nobel_response = response.json()
            else:
                print(f"Yours response code is {response.status_code}")
                raise Exception
        except Exception as err:
            nobel_response = None
            print(err)
        return nobel_response

    def fetch_laureates(self, year):
        """The method used for fetch nobel prize based on the params of endpoint"""
        try:
            if year not in range(self.min_year,self.max_year+1):
                sys.exit("Not a valid year")
            params = f"?nobelPrizeYear={year}"
            endpoint = self.laureates_endpoint + params
            response = requests.get(endpoint)
            print(response.status_code)
            if response.status_code == 200:
                nobel_response = response.json()
            else:
                print(f"Yours response code is {response.status_code}")
                raise Exception
        except Exception as err:
            nobel_response = None
            print(err)
        return nobel_response
