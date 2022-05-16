"""This module is used for fetch the nobe prizes, laureates data 
from api and upload to s3 with the year wise partition as single file"""

import os
from time import time
import pandas as pd
import configparser
import argparse
import logging
from nobelprize_api import NobelPrize_Api


parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir+'/develop.ini')

log_dir = os.path.join(parent_dir,config['local']['local_log'],'nobelprize_laureates')
os.makedirs(log_dir,exist_ok=True)
log_file = os.path.join(log_dir,'nobelprize_laureates.log')

logging.basicConfig(filename =log_file,datefmt="%d-%b-%y %H:%M:%S",
                    format="%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s",
                    level=logging.INFO)

logger = logging.getLogger()

class NobelprizeLaureates:
    """This class has the methods for getting api response for nobelprizes and laureates convert
    them into data frame filter the data frame based on the year, convert the filtered dataframe as json,
    and upload to s3 with partition year"""
    
    def __init__(self,args) -> None:
        """This is the init method for the class NobelprizeLaureates"""
        self.nobelprize_api = NobelPrize_Api(config,vars(args))
        
    def fetch_nobelprize_data(self,):
        """This method will used to fetch nobel prizes data from the api endpoint as dataframe"""
        response = self.nobelprize_api.fetch_nobel_prize()
        # self.create_json(response)
        print(response)
        
    
    def fetch_laureates_data(self,):
        """This method will used to fetch laureates data from the api endpoint as dataframe"""
        response = self.nobelprize_api.fetch_laureates()
        
        self.create_json(response)
        print(response)
        
    def create_json(self,data_frame,award_year):
        """This method will create the json file from the given dataframe"""
        try:
            epoch = int(time())
            file_name = f"{epoch}.json"
            new_df = data_frame[(data_frame.award_year == award_year)]
            new_df.to_json(self.path + "/" + file_name, orient="records", lines=True)
            # self.upload_to_s3(self.path + "/" + file_name, key)
            json_file_path = self.path + "/" + file_name
        except Exception as err:
            json_file_path = None
        return json_file_path
    
def main():
    """This the main method for the module fetch_nobelpriz_and_laureates"""
    parser = argparse.ArgumentParser(description="Argparser for get input from user")
    parser.add_argument('--nobelPrizeYear',type=int,help="Enter year for fetch data",)
    parser.add_argument('--year_to',type=int,help="Enter year_to for fetch data",)
    args = parser.parse_args()
    nobel = NobelprizeLaureates(args)
    nobel.fetch_nobelprize_data()
    
if __name__ =="__main__":
    main()
    
        
