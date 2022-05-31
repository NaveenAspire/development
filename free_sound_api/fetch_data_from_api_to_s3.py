"""This module used for fetch data from the free
sound api and upload those data as json in the s3 with partition"""

import configparser
import os
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
    
    def fetch_similar_sounds(self, sound_id):
        """This method get the similar sound of the gieven sound id
        
        Parameter :
            sound_id : The id of the sound"""
        self.free_sound_api.similar_sounds(sound_id)
        
    def fetch_user_packs(self, username):
        """This method get the packs which created by the user
        
        Parameter :
            username : The username of the user"""
        self.free_sound_api.user_packs(username)
        
def main():
    """This is the main method of this module"""
    fetch_data = FetchDataFromFreeSoundApi()
    fetch_data.fetch_similar_sounds('636061')
    # fetch_data.fetch_user_packs('Jovica')
    
if __name__ == "__main__":
    main()
        
        