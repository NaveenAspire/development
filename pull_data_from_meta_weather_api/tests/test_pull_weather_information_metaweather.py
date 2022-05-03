"""This is the test module for test  the pull data weather information from meta weather api.
The endpoint of url is 'https://www.metaweather.com/api/location/{woeid}/{date}/'"""
import sys
from typing import List
import pytest
import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from metaweather_api import MetaWeatherApi
from S3.s3 import S3Service
from pull_weather_information_from_metaweather_api import PullWeatherInformationFromMetaWeatherApi

logger = ""
endpoint = "https://www.metaweather.com/api/location/{woeid}/{date}/".format(woeid=woeid, date=date)
woeid = ""
city = ""
date = ""
s_date = ""
e_date = ""
s_date_none = None
e_date_none = None
last_run = ""
last_run_none = None


class Test_metaweather_api:
    """This class will check all success and failure test cases in metaweather api"""

    def test_metaweather_api_object(self):
        """This Method will test for the instance belong to the class MetaWeatherApi"""
        self.obj = MetaWeatherApi(logger)
        assert isinstance(self.obj, MetaWeatherApi)

    def test_weather_information_using_woeid_date_is_done(self):
        """This method will test weather information fetched is done from metaweather api"""
        self.obj = MetaWeatherApi(logger)
        response = self.obj.weather_information_using_woeid_date(woeid, date)
        try:
            result = requests.get(endpoint).json
        except Exception as err:
            print(err)
            result = None
        assert response == result

    pytest.mark.xfail

    def test_weather_information_using_woeid_date_is_not_done(self):
        """This method will test weather information fetched is not done from metaweather api"""
        self.obj = MetaWeatherApi(logger)
        response = self.obj.weather_information_using_woeid_date(wrong_woeid, wrong_date)
        try:
            result = requests.get(endpoint).json
        except Exception as err:
            print(err)
            result = None
        assert response == result


class Test_s3:
    """This class will test all success and failure cases for s3 module"""

    def test_s3_object(self):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Service(logger)
        assert isinstance(self.s3_obj, S3Service)

    def test_upload_file(self, key):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Service(logger)
        key = key
        self.my_client.upload_file(file, key)
        self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
        assert len(self.files) > 0

    @pytest.mark.xfail
    def test_upload_file_failed(self, key):
        """This method will test file is not sucessfully uploaded"""
        self.my_client = S3Service(logger)
        key = key
        self.my_client.upload_file(unavailable_file, key)
        self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
        assert len(self.files) == 0


class Test_pull_weather_information_from_metaweather_api:
    """This class will test all success and failure cases for
    pull_weather_information_from_metaweather_api module"""

    def test_pull_weather_information_from_metaweather_api_object(self):
        """This method test the instance belong to the class of PullWeatherInformationFromMetaWeatherApi"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        assert isinstance(self.s3_obj, PullWeatherInformationFromMetaWeatherApi)

    def test_get_weather_information_for_given_dates_start_and_end_done(self):
        """This method will test whether it get information for given start and end dates sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        response = self.obj.get_weather_information_for_given_dates()
        dates = []
        while s_date <= e_date:
            dates = dates.append(s_date)
            s_date = s_date + timedelta(1)
        assert response == dates

    @pytest.xfail
    def test_get_weather_information_for_given_dates_start_and_end_not_done(self):
        """This method will test whether it get information for given start and end dates
        is not done due to unavailable of date"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        response = self.obj.get_weather_information_for_given_dates()
        dates = []
        while s_date <= e_date:
            dates = dates.append(s_date)
            s_date = s_date + timedelta(1)
        assert response != dates

    def test_get_weather_information_for_given_dates_last_run_done(self):
        """This method will test whether it get information for last rund date to previous is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        check_date = datetime.now().date() - timedelta(1)
        self.obj.last_run = last_run
        response = self.obj.get_weather_information_for_given_dates()
        dates = []
        while last_run <= check_date:
            dates = dates.append(last_run)
            last_run = last_run + timedelta(1)
        assert response == dates

    @pytest.xfail
    def test_get_weather_information_for_given_dates_last_run_not_done(self):
        """This method will test whether it get information for last rund date to previous is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        self.obj.last_run = last_run_none
        check_date = datetime.now().date() - timedelta(1)
        response = self.obj.get_weather_information_for_given_dates()
        dates = []
        while last_run <= check_date:
            dates = dates.append(last_run)
            last_run = last_run + timedelta(1)
        assert response != dates

    def test_get_weather_information_for_given_dates_previous_date_done(self):
        """This method will test whether it get information for previous date is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        self.obj.last_run = last_run_none
        response = self.obj.get_weather_information_for_given_dates()
        assert type(response[0]) == dict

    def test_get_weather_information_for_given_dates_previous_date_not_done(self):
        """This method will test whether it get information for previous date is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        self.obj.last_run = last_run
        response = self.obj.get_weather_information_for_given_dates()
        assert type(response[0]) != dict
