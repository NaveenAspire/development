"""This is the test module for test  the fetch the nobel prizes and laureates year wise.
And upload the information as json file with partition based on aeard year"""
import configparser
import os
import logging
import pytest
from time import time
import pandas as pd
from pandas.testing import assert_frame_equal
import requests

from nobelprize_api import NobelPrize_Api
from S3.s3 import S3Service
from fetch_nobelprize_and_laureates import NobelprizeLaureates,parent_dir

log_dir = os.path.join(parent_dir, "opt/logging/fetch_nobel_prizes_laureates/")
log_file = os.path.join(log_dir, "fetch_nobel_prizes_laureates.log")
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
def api_res(endpoint):
    response = requests.get(endpoint).json()
    print(response)
    return response

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


class Test_nobelPrize_api:
    """This class will check all success and failure test cases in nobelPrize api"""

    def test_metaweather_api_object(self):
        """This Method will test for the instance belong to the class MetaWeatherApi"""
        self.obj = NobelPrize_Api(config)
        assert isinstance(self.obj, NobelPrize_Api)

    def test_fetch_nobel_prize_is_done(self, endpoint,year):
        """This method will test fetch nobel prizes is done given year"""
        self.obj = NobelPrize_Api(config)
        try:
            response = self.obj.fetch_nobel_prize(year)
            result = requests.get(endpoint).json()
            print(result)
        except Exception as err:
            print(err)
            result = None
        assert response == result

    @pytest.mark.xfail
    def test_fetch_nobel_prize_not_done(self, endpoint,wrong_year):
        """This method will test fetch nobel prizes is not done for given year"""
        self.obj = NobelPrize_Api(config)
        try:
            response = self.obj.fetch_nobel_prize(wrong_year)
            result = requests.get(endpoint).json()
            print(result)
        except Exception as err:
            print(err)
            result = None
        assert response == result
        
    def test_fetch_laureates_is_done(self, endpoint,year):
        """This method will test fetch laureates is done for given year"""
        self.obj = NobelPrize_Api(config)
        try:
            response = self.obj.fetch_nobel_prize(year)
            result = requests.get(endpoint).json()
            print(result)
        except Exception as err:
            print(err)
            result = None
        assert response == result
        
    @pytest.mark.xfail
    def test_fetch_laureates_not_done(self, endpoint,wrong_year):
        """This method will test fetch laureates is not done for given year"""
        self.obj = NobelPrize_Api(config)
        try:
            response = self.obj.fetch_nobel_prize(wrong_year)
            result = requests.get(endpoint).json()
            print(result)
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

    def test_upload_file(self,partition,key,bucket,bucket_path,file):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Service(logger)
        self.my_client.upload_file(file, key)
        self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
        assert len(self.files) > 0

    @pytest.mark.xfail
    def test_upload_file_failed(self,key,bucket,bucket_path,partition,unavailable_file):
        """This method will test file is not sucessfully uploaded"""
        self.my_client = S3Service(logger)
        self.my_client.upload_file(unavailable_file, key)
        self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
        assert len(self.files) == 0


class Test_fetch_nobelPrizes_laureates:
    """This class will test all success and failure cases for
    fetch_nobel_prizes_laureates module"""

    def test_nobel_prizes_laureates_object(self,args):
        """This method test the instance belong to the class of NobelprizeLaureates"""
        self.obj = NobelprizeLaureates(args)
        assert isinstance(self.obj, NobelprizeLaureates)

    def test_fetch_nobelprize_data_done(self, s_date, e_date):  # --tested
        """This method will test whether it get weather information is sucessful"""
        self.obj = NobelprizeLaureates(args)
        response = self.obj.fetch_nobelprize_data()
        assert isinstance(response, list)

    @pytest.mark.xfail  # tested
    def test_fetch_nobelprize_data_not_done(self, s_date, e_date):  # --tested
        """This method will test whether it get weather information is sucessful"""
        self.obj = NobelprizeLaureates(args)
        response = self.obj.fetch_nobelprize_data()
        assert isinstance(response, list)

    def test_fetch_nobelprize_data_done(self, s_date, e_date):  # --tested
        """This method will test whether it get weather information is sucessful"""
        self.obj = NobelprizeLaureates(args)
        response = self.obj.fetch_laureates_data()
        assert isinstance(response, list)

    @pytest.mark.xfail  # tested
    def test_fetch_nobelprize_data_not_done(self, s_date, e_date):  # --tested
        """This method will test whether it get weather information is sucessful"""
        self.obj = NobelprizeLaureates(args)
        response = self.obj.fetch_laureates_data()
        assert isinstance(response, list)

    def test_create_json_file_done(self, s_date, e_date, api_res, city):  # --tested
        """This method will test whether it create json file is sucessful"""
        self.obj = NobelprizeLaureates(args)
        res_df = pd.DataFrame(api_res)
        response = self.obj.create_json_file(new_df, city, t_str)
        assert os.path.isfile(response)

    @pytest.mark.xfail
    def test_create_json_file_done(self, s_date, e_date, api_res, city):  # --tested
        """This method will test whether it create json file is sucessful"""
        self.obj = NobelprizeLaureates(args)
        res_df = pd.DataFrame(api_res)
        response = self.obj.create_json_file(new_df, city, t_str)
        assert os.path.isfile(response)

    def test_get_partition_path_done(self):  # --tested
        """This method will test whether it get paartition is sucessful"""
        self.obj = NobelprizeLaureates(args)
        response = self.obj.get_partition(award_year)
        assert response == partition_path

    def test_get_partition_path_done(self):  # --tested
        """This method will test whether it get partition is sucessful"""
        self.obj = NobelprizeLaureates(args)
        response = self.obj.get_partition(award_year)
        assert response == partition_path
