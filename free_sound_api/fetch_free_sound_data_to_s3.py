"""This module used for fetch data from the free
sound api and upload those data as json in the s3 with partition"""

import argparse
import configparser
from datetime import datetime
import os
import shutil
import sys
from S3.s3 import S3Service
from Temp_s3.temp_s3 import TempS3
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
        # self.s3_service = S3Service(logger)
        self.temp_s3 = TempS3(config, logger)
        self.section = config["fetch_free_sound_data"]

    def fetch_similar_sounds(self, sound_id):
        """This method get the similar sound of the gieven sound id

        Parameter :
            sound_id : The id of the sound"""
        try:
            response = self.free_sound_api.similar_sounds(sound_id)
            logger.info("response successfuly get from similar sound endpoint...")
            today = datetime.strftime(datetime.now().today().date(), "%Y-%m-%d")
            if response is None:
                sys.exit("Data does not get from api")

            create_json = self.create_json_file(
                response, f"similar_sound_for_{sound_id}.json"
            )
            # key = os.path.join(
            #     self.section.get("similar_sound_bpath"),
            #     self.get_partition(today,id=sound_id),
            #     f"{sound_id}.json",
            # )
            # self.s3_service.upload_file(create_json, key)
            self.temp_s3.upload_local_s3(
                create_json,
                os.path.join(
                    self.section.get("similar_sound_bpath"),
                    self.get_partition(today, id=sound_id),
                ),
            )
            # shutil.rmtree(self.download_path)
            logger.info("Download Folder cleaned successfully...")
        except Exception as err:
            logger.exception(err)
            error = err
            print(error)
        return not "error" in locals()

    def fetch_user_packs(self, username):
        """This method get the packs which created by the user

        Parameter :
            username : The username of the user"""
        try:
            response = self.free_sound_api.user_packs(username)
            logger.info("response successfuly get from user packs endpoint...")
            if response is None:
                sys.exit("Data does not get from api")
            unique_list = response.created.unique()
            for unique in unique_list:
                new_df = response[(response.created == unique)]
                if not new_df.empty:
                    pack_id = new_df.id.values[0]
                    file_name = f"userpacks_{pack_id}_{(unique.split('T')[0]).replace('-','')}.json"
                    create_json = self.create_json_file(new_df, file_name)
                    print(file_name)
                    # key = os.path.join(
                    #     self.section.get("user_packs_bpath"),
                    #     self.get_partition(unique.split("T")[0],id=pack_id),
                    #     file_name,
                    # )
                    # self.s3_service.upload_file(create_json,key)
                    self.temp_s3.upload_local_s3(
                        create_json,
                        os.path.join(
                            self.section.get("user_packs_bpath"),
                            self.get_partition(unique.split("T")[0]),
                        ),
                    )
            shutil.rmtree(self.download_path)
            logger.info("Download Folder cleaned successfully...")
        except Exception as err:
            logger.exception(err)
            error = err
            print(error)
        return not "error" in locals()

    def create_json_file(self, data_frame, file_name):
        """This method create the json file for the given dataframe as name given
        Parameter :
            data_frame : The dataframe need to be create as json file
            file_name : The name of the file which going to create"""
        try:
            data_frame.to_json(
                os.path.join(self.download_path, file_name),
                orient="records",
                lines=True,
            )
            file_path = os.path.join(self.download_path, file_name)
            logger.info("Json file created successfully as name %s", file_name)
        except Exception as err:
            print(err)
            file_path = None
        return file_path

    def get_partition(self, partition_variable, id=None):
        """This method will make the partition based on given date
        Parameter :
            partition_variable : The date that to be partition"""
        try:
            print("id :", id)
            date_obj = datetime.strptime(partition_variable, "%Y-%m-%d")
            partition_path = (
                date_obj.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d/")
                if not id
                else date_obj.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d/pt_id={id}")
            )

        except Exception as err:
            print(err)
            sys.exit(f"Script stopped due to {err}")
        return partition_path


def main():
    """This is the main method of this module"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sound_id", type=str, help="Enter sound id for fetch similar sounds"
    )
    parser.add_argument(
        "--username", type=str, help="Enter username for fetch user packs"
    )
    parser.add_argument(
        "endpoint",
        choices=["sound_similar", "user_packs"],
        help="Choose from choices for get single endpoint response either 'sound_similar' or 'user_packs'",
    )
    args = parser.parse_args()
    fetch_data = FetchDataFromFreeSoundApi()
    if args.endpoint == "sound_similar" and args.sound_id:
        fetch_data.fetch_similar_sounds(args.sound_id)
    elif args.endpoint == "user_packs" and args.username:
        fetch_data.fetch_user_packs(args.username)
    else:
        print("Please Give input for the Choosed endpoint...")


if __name__ == "__main__":
    main()
