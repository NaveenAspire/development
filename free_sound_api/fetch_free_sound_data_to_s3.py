"""This module used for fetch data from the free
sound api and upload those data as json in the s3 with partition"""

import argparse
import configparser
from datetime import datetime, timedelta
import os
import sys
from S3.s3 import S3Service
from logging_and_download_path import LoggingDownloadpath, parent_dir
from free_sound_api import FreeSoundApi

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_free_sound_api_data")

class FetchDataFromFreeSoundApi:
    """This class for fetching data from free sound api and
    upload the response as json file into s3 with partition"""
    
    def __init__(self) -> None:
        """
        This is init method for the class FetchDataFromFreeSoundApi
        
        Parameter :
            """
        self.free_sound_api = FreeSoundApi(config)
        self.download_path = logger_download.set_downloadpath("free_sound_api")
        self.s3_service = S3Service(logger)
        self.similar_sound_path = config['fetch_free_sound_data']['similar_sound_bpath']
        self.user_packs_path = config['fetch_free_sound_data']['user_packs_bpath']
    
    def fetch_similar_sounds(self, sound_id):
        """This method get the similar sound of the gieven sound id
        
        Parameter :
            sound_id : The id of the sound"""
        response = self.free_sound_api.similar_sounds(sound_id)
        today = datetime.strftime(datetime.now().today().date(),"%Y%-m-%d")
        if response :
            create_json = self.create_json_file(response,f"{sound_id}.json")
            key = os.path.join(self.similar_sound_path,self.get_partition(today),f"{sound_id}.json")
            self.s3_service.upload_file(create_json,key)
        
    def fetch_user_packs(self, username):
        """This method get the packs which created by the user
        
        Parameter :
            username : The username of the user"""
        response = self.free_sound_api.user_packs(username)
        if not response :
            sys.exit("Data does not get from api")
        start = min(response["created"])
        while start <= max(response["created"]):
            new_df = response[(response.created == start)]
            if not new_df.empty:
                create_json = self.create_json_file(new_df, start)
                key = os.path.join(self.similar_sound_path,self.get_partition(start),f"{start}.json")
                self.s3_service.upload_file(create_json,key)
            start = start + timedelta(1)
    
    def create_json_file(self, data_frame, file_name):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file
            file_name : The name of the file which going to create"""
        data_frame.to_json(os.path.join(self.download_path, file_name),
                           orient="records",
                           lines=True,)
        file_path = os.path.join(self.download_path,file_name)
        return file_path
        
    def get_partition(self,partition_variable):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            date_obj = datetime.strptime(partition_variable, "%Y-%m-%d")
            partition_path = date_obj.strftime(
                f"pt_year=%Y/pt_month=%m/pt_day=%d/"
            )
        except Exception as err:
            print(err)
            partition_path = None
        return partition_path
        
def main():
    """This is the main method of this module"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--sound_id',type=int,help='Enter sound id for fetch similar sounds')
    parser.add_argument('--username',type=int,help='Enter username for fetch user packs')
    args = parser.parse_args()
    fetch_data = FetchDataFromFreeSoundApi()
    fetch_data.fetch_similar_sounds(args.sound_id)
    fetch_data.fetch_user_packs(args.username)
    
if __name__ == "__main__":
    main()
        
        