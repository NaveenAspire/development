"""This module used for fetch data from nobelprize api based on the endpoint called"""
import sys
from datetime import date
import requests


class NobelPrizeApi:
    """This is the class which contains methods for each endpoint to fetch data"""

    def __init__(self, config_obj, logger_obj) -> None:
        """This is the init method for the class NobelPrize_Api"""
        self.nobelprize_endpoint = config_obj["nobel_api"]["nobelprize_endpoint"]
        self.laureates_endpoint = config_obj["nobel_api"]["laureates_endpoint"]
        today_date = date.today()
        self.min_year = 1901
        self.max_year = (
            today_date.year
            if today_date.month >= 12 and today_date.day > 10
            else today_date.year - 1
        )
        self.logging = logger_obj

    def fetch_prize_or_laureaes_response(self, endpoint, year):
        """The method used for fetch nobel prize based on the params of endpoint"""
        try:
            if year not in range(self.min_year, self.max_year + 1):
                # return None # for testing
                sys.exit(f"No data available for the year {year}")
            params = f"?nobelPrizeYear={year}"
            response = requests.get(endpoint + params)
            if response.status_code == 200:
                nobel_response = response.json()
                self.logging.info(f"nobel prize response get sucessfully for the year {year}")
            else:
                print(f"Yours response code is {response.status_code}")
                raise Exception
        except Exception as err:
            nobel_response = None
            print(err)
            self.logging.error(f"nobel prize response is not fetch for year {year} due to {err}")
        return nobel_response
