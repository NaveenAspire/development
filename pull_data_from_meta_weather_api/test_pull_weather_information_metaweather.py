"""This is the test module for test  the pull data weather information from meta weather api.
The endpoint of url is 'https://www.metaweather.com/api/location/{woeid}/{date}/'"""
import configparser
import os
import logging
import pytest
import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from metaweather_api import MetaWeatherApi
from S3.s3 import S3Service
from pull_weather_information_from_metaweather_api import (
    PullWeatherInformationFromMetaWeatherApi,
    get_date,
    set_last_run,
    parent_dir,
)


log_dir = os.path.join(parent_dir, "opt/logging/pull_weather_information_from_metaweather_api/")
log_file = os.path.join(log_dir, "test_weather_information.log")
logging.basicConfig(
    filename=log_file,
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    force=True,
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")


@pytest.fixture
def woeid():
    return 2471390


@pytest.fixture
def city():
    return "Phoenix"


@pytest.fixture
def woeid():
    return 2471390


@pytest.fixture
def wrong_woeid():
    return 12345


@pytest.fixture
def api_search_date():
    yesterday = datetime.now().date() - timedelta(1)
    str_date = datetime.strftime(yesterday, "%Y/%m/%d")
    return str_date


@pytest.fixture
def date_str():
    yesterday = datetime.now().date() - timedelta(1)
    str_date = datetime.strftime(yesterday, "%Y-%m-%d")
    return str_date


@pytest.fixture
def wrong_api_search_date():
    yesterday = datetime.now().date() - timedelta(1)
    str_date = datetime.strftime(yesterday, "%d/%m/%Y")
    return str_date


@pytest.fixture
def endpoint(woeid, api_search_date):
    return f"https://www.metaweather.com/api/location/{woeid}/{api_search_date}/"


@pytest.fixture
def s_date():
    str_date = "2022-05-01"
    start_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return start_date


@pytest.fixture
def e_date():
    str_date = "2022-05-03"
    end_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return end_date


@pytest.fixture
def s_date_none():
    return None


@pytest.fixture
def e_date_none():
    return None


@pytest.fixture
def last_run():
    str_date = config["pull_weather_information_from_metaweather_api"]["last_date"]
    last_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return last_date


@pytest.fixture
def last_run_none():
    return None


@pytest.fixture
def api_res(endpoint):
    response = requests.get(endpoint).json()
    print(response)
    return response


@pytest.fixture
def t_str(api_res, s_date):
    res_df = pd.DataFrame(api_res)
    for i in range(24):
        t_str = "{date}T{hour}".format(date=s_date, hour=str(i).zfill(2))
        new_df = res_df[(res_df.created.str.contains(t_str))]
        if not new_df.empty:
            break
    return t_str


@pytest.fixture
def partition(t_str):
    date_obj = datetime.strptime(t_str, "%Y-%m-%dT%H")
    partition_path = date_obj.strftime(
        f"pt_city={city}/pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/"
    )
    return partition_path


@pytest.fixture
def file_name(city):
    epoch = int(time())
    file_name = f"metaweather_{city}_{epoch}.json"
    res_df = pd.DataFrame(api_res)
    for i in range(24):
        t_str = "{date}T{hour}".format(date=s_date, hour=str(i).zfill(2))
        new_df = res_df[(res_df.created.str.contains(t_str))]
        if not new_df.empty:
            break
    path = os.path.join(parent_dir, config["local"]["local_file_path"], "meta_weather")
    new_df.to_json(path + "/" + file_name, orient="records", lines=True)
    return file_name


@pytest.fixture
def file(file_name):
    file_path = os.path.join(
        parent_dir, config["local"]["local_file_path"], "meta_weather", file_name
    )
    return file_path


@pytest.fixture
def key(partition, file_name):
    date_obj = datetime.strptime(t_str, "%Y-%m-%dT%H")
    key_name = partition + file_name
    return key_name


@pytest.fixture
def bucket():
    bucket_name = config.get("s3", "bucket_name")
    return bucket_name


@pytest.fixture
def bucket_path():
    bucket_path_name = config.get("s3", "bucket_path")
    return bucket_path_name


class Test_metaweather_api:
    """This class will check all success and failure test cases in metaweather api"""

    def test_metaweather_api_object(self):
        """This Method will test for the instance belong to the class MetaWeatherApi"""
        self.obj = MetaWeatherApi(logger)
        assert isinstance(self.obj, MetaWeatherApi)

    def test_weather_information_using_woeid_date_is_done(self, endpoint, woeid, api_search_date):
        """This method will test weather information fetched is done from metaweather api"""
        self.obj = MetaWeatherApi(logger)
        try:
            response = self.obj.weather_information_using_woeid_date(woeid, api_search_date)
            result = requests.get(endpoint).json()
            print(result)
        except Exception as err:
            print(err)
            result = None
        assert response == result

    @pytest.mark.xfail
    def test_weather_information_using_woeid_date_is_not_done(
        self, wrong_woeid, wrong_api_search_date
    ):
        """This method will test weather information fetched is not done from metaweather api"""
        self.obj = MetaWeatherApi(logger)
        response = self.obj.weather_information_using_woeid_date(wrong_woeid, wrong_api_search_date)
        try:
            result = requests.get(endpoint).json()
        except Exception as err:
            print(err)
            result = None
        assert response == result


# class Test_s3:
#     """This class will test all success and failure cases for s3 module"""

#     def test_s3_object(self):
#         """This method test the instance belong to the class of S3Service"""
#         self.s3_obj = S3Service(logger)
#         assert isinstance(self.s3_obj, S3Service)

#     def test_upload_file(self,partition,key,bucket,bucket_path,file):
#         """This method will test file is sucessfully uploaded"""
#         self.my_client = S3Service(logger)
#         self.my_client.upload_file(file, key)
#         self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
#         assert len(self.files) > 0

#     @pytest.mark.xfail
#     def test_upload_file_failed(self,key,bucket,bucket_path,partition,unavailable_file):
#         """This method will test file is not sucessfully uploaded"""
#         self.my_client = S3Service(logger)
#         self.my_client.upload_file(unavailable_file, key)
#         self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
#         assert len(self.files) == 0


class Test_pull_weather_information_from_metaweather_api:
    """This class will test all success and failure cases for
    pull_weather_information_from_metaweather_api module"""

    def test_pull_weather_information_from_metaweather_api_object(self, s_date, e_date):  # --tested
        """This method test the instance belong to the class of PullWeatherInformationFromMetaWeatherApi"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        assert isinstance(self.obj, PullWeatherInformationFromMetaWeatherApi)

    def test_get_weather_information_for_given_dates_start_and_end_done(
        self, s_date, e_date
    ):  # --tested
        """This method will test whether it get information for given start and end dates sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        response = self.obj.get_weather_information_for_given_dates()
        print(response)
        dates = []
        while s_date <= e_date:
            dates.append(s_date)
            s_date = s_date + timedelta(1)
        print(dates)
        print(response)
        assert response == dates

    @pytest.mark.xfail  # --tested
    def test_get_weather_information_for_given_dates_start_and_end_not_done(
        self, s_date, e_date, s_date_none, e_date_none
    ):
        """This method will test whether it get information for given start and end dates
        is not done due to unavailable of date"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        response = self.obj.get_weather_information_for_given_dates()
        dates = []
        while s_date <= e_date:
            dates.append(s_date)
            s_date = s_date + timedelta(1)
        assert response != dates

    def test_get_weather_information_for_given_dates_last_run_done(
        self, s_date_none, e_date_none, last_run
    ):  # --tested
        """This method will test whether it get information for last rund date to previous is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        print(self.obj)
        check_date = datetime.now().date() - timedelta(1)
        self.obj.last_run = last_run
        response = self.obj.get_weather_information_for_given_dates()
        dates = []
        while last_run <= check_date:
            dates.append(last_run)
            last_run = last_run + timedelta(1)
        assert response == dates

    @pytest.mark.xfail  # --tested
    def test_get_weather_information_for_given_dates_last_run_not_done(
        self, s_date_none, e_date_none, last_run_none, last_run
    ):
        """This method will test whether it get information for last rund date to previous is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        self.obj.last_run = last_run_none
        check_date = datetime.now().date() - timedelta(1)
        response = self.obj.get_weather_information_for_given_dates()
        dates = []
        while last_run <= check_date:
            dates.append(last_run)
            last_run = last_run + timedelta(1)
        assert response != dates

    def test_get_weather_information_for_given_dates_previous_date_done(
        self, s_date_none, e_date_none, last_run_none
    ):  # --tested
        """This method will test whether it get information for previous date is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        self.obj.last_run = last_run_none
        print(self.obj.last_run)
        response = self.obj.get_weather_information_for_given_dates()
        assert type(response[0]) == dict

    @pytest.mark.xfail  # --tested
    def test_get_weather_information_for_given_dates_previous_date_not_done(
        self, s_date_none, e_date_none, last_run
    ):
        """This method will test whether it get information for previous date is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        self.obj.last_run = last_run
        response = self.obj.get_weather_information_for_given_dates()
        assert type(response[0]) != dict

    def test_get_weather_information_between_two_dates_done(self, s_date, e_date):  # --tested
        """This method will test whether it get information between two days is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        response = self.obj.get_weather_information_between_two_dates(s_date, e_date)
        assert isinstance(response, list)

    @pytest.mark.xfail  # --tested
    def test_get_weather_information_between_two_dates_not_done(
        self, s_date_none, e_date_none, last_run_none
    ):
        """This method will test whether it get information between two days is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date_none)
        self.obj.last_run = last_run_none
        response = self.obj.get_weather_information_between_two_dates(s_date_none, e_date_none)
        assert not isinstance(response, list)

    def test_get_weather_information_done(self, s_date, e_date):  # --tested
        """This method will test whether it get weather information is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        response = self.obj.get_weather_information(s_date)
        assert isinstance(response, list)

    @pytest.mark.xfail  # tested
    def test_get_weather_information_not_done(self, s_date_none, e_date):
        """This method will test whether it get weather information is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date)
        response = self.obj.get_weather_information(s_date_none)
        print(response)
        assert response is None

    def test_get_given_date_response_done(self, s_date, e_date, api_res, city):  # --tested
        """This method will test whether it get_given_date_response is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        response = self.obj.get_given_date_response(api_res, city, s_date)
        assert response is True

    @pytest.mark.xfail
    def test_get_given_date_response_not_done(self, s_date_none, e_date, api_res, city):  # --tested
        """This method will test whether it get_given_date_response is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date_none, e_date)
        response = self.obj.get_given_date_response(api_res, city, s_date_none)
        assert response is False

    def test_create_json_file_done(self, s_date, e_date, api_res, city):  # --tested
        """This method will test whether it create json file is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        res_df = pd.DataFrame(api_res)
        for i in range(24):
            t_str = "{date}T{hour}".format(date=s_date, hour=str(i).zfill(2))
            new_df = res_df[(res_df.created.str.contains(t_str))]
            if not new_df.empty:
                break
        response = self.obj.create_json_file(new_df, city, t_str)
        assert os.path.isfile(response)

    @pytest.mark.xfail
    def test_create_json_file_not_done(self, city, s_date, e_date, t_str):  # --tested
        """This method will test whether it create json file is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        new_df = "saa"  # passing dataframe as a string for failure case
        response = self.obj.create_json_file(new_df, city, t_str)
        assert response is None

    def test_get_partition_path_done(self, city, s_date, e_date, t_str):  # --tested
        """This method will test whether it get paartition is sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        try:
            print(t_str)
            date_obj = datetime.strptime(t_str, "%Y-%m-%dT%H")
            partition_path = date_obj.strftime(
                f"pt_city={city}/pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/"
            )
        except Exception as err:
            partition_path = None
        response = self.obj.get_partition_path(city, t_str)
        assert response == partition_path

    @pytest.mark.xfail
    def test_get_partition_path_not_done(self, s_date, e_date, city, t_str):  # --tested
        """This method will test whether it get paartition is not sucessful"""
        self.obj = PullWeatherInformationFromMetaWeatherApi(s_date, e_date)
        try:
            date_obj = datetime.strptime(
                "sfafs", "%Y-%m-%dT%H"
            )  # t_str will be wrong format for failure case
            partition_path = date_obj.strftime(
                f"pt_city={city}/pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/"
            )
        except Exception as err:
            partition_path = None
        response = self.obj.get_partition_path(
            city, "sfsafa"
        )  # t_str will be wrong format for failure case
        assert response == partition_path

    def test_get_date_done(self, date_str):  # tested
        """This method will test whether get date is successful"""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception as err:
            date_obj = None
        response = get_date(date_str)
        assert response == date_obj

    @pytest.mark.xfail
    def test_get_date_not_done(self, date_str):  # --tested
        """This method will test whether get date is not successful"""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception as err:
            date_obj = None
        response = get_date(date_str)
        assert response == date_obj

    def test_set_last_run_done(self):  # tested
        """This method wil test the function which update last run in config file sucessfully"""
        set_last_run()
        config = configparser.ConfigParser()
        config.read(parent_dir + "/develop.ini")
        last_run_date = config.get("pull_weather_information_from_metaweather_api", "last_date")
        assert last_run_date == str(datetime.now().date())

    @pytest.mark.xfail  # tested
    def test_set_last_run_not_done(self):
        """This method wil test the function which update last run in config file sucessfully"""
        set_last_run()
        config = configparser.ConfigParser()
        config.read(parent_dir + "/develop.ini")
        last_run_date = config.get("pull_weather_information_from_metaweather_api", "last_date")
        assert last_run_date != str(datetime.now().date())
