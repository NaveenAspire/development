"""This is the test module for test  the pull data weather information from meta weather api.
The endpoint of url is 'https://www.metaweather.com/api/location/{woeid}/{date}/'"""
import pytest
import requests
import time
import pandas as pd
from datetime import datetime
from metaweather_api import MetaWeatherApi
from S3.s3 import S3Service
from pull_weather_information_from_metaweather_api import PullWeatherInformationFromMetaWeatherApi

logger=''
woeid = ''
city = ''
date = ''
endpoint = "https://www.metaweather.com/api/location/{woeid}/{date}/".format(woeid=woeid,date=date)


# @pytest.fixture
# def partition_path(city, t_str):
#     """This method will make key name for uploading into s3 with partition"""
#     try:
#         date_obj = datetime.strptime(t_str, "%Y-%m-%dT%H")
#         partition_path = date_obj.strftime(
#             f"pt_city={city}/pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/")
#     except Exception as err:
#         print(err)
#     return partition_path


# @pytest.fixture
# def s3_key(response,city):
#     """This is the fixture for mocking s3 service"""
#     date = date.strftime("%Y-%m-%d")
#     res_df = pd.DataFrame(response)
#     for i in range(24):
#         t_str = "{date}T{hour}".format(date=date, hour=str(i).zfill(2))
#         new_df = res_df[(res_df.created.str.contains(t_str))]
#         if not new_df.empty:
#             epoch = int(time())
#         file_name = f"metaweather_{city}_{epoch}.json"
#         new_df.to_json(self.path + "/" + file_name, orient="records", lines=True)
#         key = self.get_partition_path(city, t_str) + "/" + file_name


class Test_metaweather_api:
    """This class will check all success and failure test cases in metaweather api"""
    
    def test_metaweather_api_object(self):
        """ This Method will test for the instance belong to the class MetaWeatherApi """
        self.obj = MetaWeatherApi(logger)
        assert isinstance(self.obj,MetaWeatherApi)
        
    def test_weather_information_using_woeid_date_is_done(self):
        """This method will test weather information fetched is done from metaweather api"""
        self.obj = MetaWeatherApi(logger)
        response = self.obj.weather_information_using_woeid_date(woeid,date)
        try :
            result = requests.get(endpoint).json
        except Exception as err :
            print(err)
            result = None
        assert response == result
    
    pytest.mark.xfail
    def test_weather_information_using_woeid_date_is_not_done(self):
        """This method will test weather information fetched is not done from metaweather api"""
        self.obj = MetaWeatherApi(logger)
        response = self.obj.weather_information_using_woeid_date(wrong_woeid,wrong_date)
        try :
            result = requests.get(endpoint).json
        except Exception as err :
            print(err)
            result = None
        assert response == result
    
class Test_s3:
    """This class will test all success and failure cases for s3 module"""
    
    def test_s3_object(self):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Service(logger)
        assert isinstance(self.s3_obj, S3Service)

    def test_upload_file(self,key):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Service(logger)
        key = key
        self.my_client.upload_file(file,key)
        self.files = self.my_client.s3_obj.list_files(bucket, bucket_path+"/"+partition)
        assert len(self.files) > 0

    @pytest.mark.xfail
    def test_upload_file_failed(self,key):
        """This method will test file is not sucessfully uploaded"""
        self.my_client = S3Service(logger)
        key = key
        self.my_client.upload_file(unavailable_file,key)
        self.files = self.my_client.s3_obj.list_files(bucket, bucket_path+"/"+partition)
        assert len(self.files) == 0
    
class Test_pull_weather_information_from_metaweather_api:
    """This class will test all success and failure cases for pull_weather_information_from_metaweather_api module"""
    
    def test_pull_weather_information_from_metaweather_api_object(self):
        """This method test the instance belong to the class of PullWeatherInformationFromMetaWeatherApi"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date,e_date)
        assert isinstance(self.s3_obj, PullWeatherInformationFromMetaWeatherApi)
        
    def test_get_weather_information_for_given_dates(self):
        """This method will test whether """
    
        
        