"""This module will test the all classes and methods
of the task fetch data from sunrise_sunset sound api and upload to s3"""

from sunrise_sunset_api import SunriseSunset
from fetch_sunrise_sunset import FetchDataFromSunriseSunsetApi
import configparser
from logging_and_download_path import LoggingDownloadpath, parent_dir
from datetime import datetime
import pytest
from moto import mock_s3
import pandas as pd
import boto3
import os
from Temp_s3.temp_s3 import TempS3
from S3.s3 import S3Service

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
logger_donload = LoggingDownloadpath(config)
logger = logger_donload.set_logger("test_sunrise_sunset_api")

sunrise_sunset_api_section = config["sunrise_sunset_api"]


@pytest.fixture
def date():
    return datetime.strptime("2022-05-02", "%Y-%m-%d").date()


@pytest.fixture
def wrong_date():
    return "2022-05-02T"


@pytest.fixture
def lat():
    return "20.5937"


@pytest.fixture
def lng():
    return "78.9629"


@pytest.fixture
def params(lat, lng, date):
    params = {"lat": lat, "lng": lng, "date": date}
    return params


@pytest.fixture
def data_frame(params):
    sunrise_sunset = SunriseSunset(config)
    res = sunrise_sunset.get_sunrise_sunset_details(params)
    return res


@pytest.fixture
def partition_path(date):
    partition_path = date.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d/")
    return partition_path


@pytest.fixture
def download_path():
    log_and_download = LoggingDownloadpath(config)
    path = log_and_download.set_downloadpath("test_free_sound_api")
    return path


@pytest.fixture
def source_path(data_frame, file_name, download_path):
    data_frame.to_json(
        os.path.join(download_path, file_name),
        orient="records",
        lines=True,
    )
    file_path = os.path.join(download_path, file_name)
    return file_path


@pytest.fixture
def wrong_params(lat, lng):
    params = {"lat": lat, "lng": lng, "date": "wrong_date"}
    return params


@pytest.fixture
def path():
    bucket_path = "source"
    return bucket_path


@pytest.fixture
def file_name(lat, lng):
    file_name = f"lat_{lat}_lng_{lng}.json"
    return file_name


# @pytest.fixture
# def file(source_path, file_name):
#     return os.path.join(source_path,file_name)


@pytest.fixture
def key(partition_path, file_name):
    key_name = partition_path + file_name
    return key_name


@pytest.fixture
def bucket():
    bucket_name = config.get("s3", "bucket_name")
    return bucket_name


@pytest.fixture
def bucket_path():
    bucket_path_name = config.get("s3", "bucket_path")
    return bucket_path_name


@pytest.fixture
def s3_client():
    """This is the fixture for mocking s3 service"""
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        print(conn)
        yield conn


class Test_s3:
    """This class will test all success and failure cases for s3 module"""

    @pytest.fixture
    def s3_test(self, s3_client, bucket):
        """This is the fixture for creating s3 bucket"""
        print(s3_client)
        self.res = s3_client.create_bucket(Bucket=bucket)
        yield

    def test_s3_object(self):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Service(logger)
        assert isinstance(self.s3_obj, S3Service)

    def test_upload_file_done(self, key, source_path, s3_client, s3_test):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Service(logger)
        response = self.my_client.upload_file(source_path, key)
        assert response == key

    @pytest.mark.xfail
    def test_upload_file_not_done(self, key, s3_client, s3_test):
        """This method will test file is not sucessfully uploaded"""
        self.my_client = S3Service(logger)
        response = self.my_client.upload_file("file", key)
        print(response)
        assert not response


class Test_sunrise_sunset_api:
    """This class will test all success and failure cases for sunrise_sunset api module"""

    def test_sunrise_sunset_api_object(self):
        """This method will test the instance belongs to the class of SunriseSunset"""
        self.sunrise_sunset = SunriseSunset(config)
        assert isinstance(self.sunrise_sunset, SunriseSunset)

    def test_get_sunrise_sunset_details_is_done(self, params):
        """This method will test whether the
        get sunrise_sunset details succesfully done for the enpoint"""
        self.sunrise_sunset = SunriseSunset(config)
        response = self.sunrise_sunset.get_sunrise_sunset_details(params)
        return isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_sunrise_sunset_details_not_done(self, wrong_params):
        """This method will test whether the
        get sunrise_sunset details not succesfully done for the enpoint"""
        self.sunrise_sunset = SunriseSunset(config)
        response = self.sunrise_sunset.get_sunrise_sunset_details(wrong_params)
        return not response


class Test_FetchDataFromSunriseSunsetApi:
    """This test class will test the all
    methods in the class FetchDataFromSunriseSunsetApi"""

    def test_fetch_data_from_sunrise_sunset_api_object(self):
        """This method will test the instance
        belongs to the class of FetchDataFromSunriseSunsetApi"""
        self.fetch_data = FetchDataFromSunriseSunsetApi()
        assert isinstance(self.fetch_data, FetchDataFromSunriseSunsetApi)

    def test_fetch_sunrise_sunset_data_is_done(self, date, lat, lng):
        """This method will test whether successfully done
        fetch sunrise and sunset details for the given date"""
        self.fetch_data = FetchDataFromSunriseSunsetApi()
        response = self.fetch_data.fetch_sunrise_sunset_data(date, lat, lng)
        assert response

    @pytest.mark.xfail
    def test_fetch_sunrise_sunset_data_is_not_done(self, wrong_date, lat, lng):
        """This method will test whether not
        successfully fetch sunrise and sunset details for the given date"""
        self.fetch_data = FetchDataFromSunriseSunsetApi()
        response = self.fetch_data.fetch_sunrise_sunset_data(wrong_date, lat, lng)
        print(response)
        assert not response

    def test_create_json_is_done(self, data_frame, date, lat, lng):
        """This method will test whether the json file created successfully."""
        self.fetch_data = FetchDataFromSunriseSunsetApi()
        response = self.fetch_data.create_json_file(data_frame, date, lat, lng)
        assert response

    @pytest.mark.xfail
    def test_create_json_file_not_done(self, data_frame, date, lat, lng):
        """This method will test whether the json file not created successfully"""
        self.fetch_data = FetchDataFromSunriseSunsetApi()
        data_frame = None
        response = self.fetch_data.create_json_file(data_frame, date, lat, lng)
        assert not response

    def test_get_partition_done(self, date):
        """This method will test whether partition get successfully"""
        self.fetch_data = FetchDataFromSunriseSunsetApi()
        response = self.fetch_data.get_partition(date)
        assert response

    @pytest.mark.xfail
    def test_get_partition_not_done(self, wrong_date):
        """This method will test whether get partition not successfully"""
        self.fetch_data = FetchDataFromSunriseSunsetApi()
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            response = self.fetch_data.get_partition(wrong_date)
            assert pytest_wrapped_e.type == SystemExit
            assert pytest_wrapped_e.value.code == 42


class Test_temp_s3:
    """This class will test all the methods in temp_s3 module"""

    def test_temp_s3_object(self):
        """This method will test the instance belongs to the class of TempS3"""
        self.temp_s3 = TempS3(config, logger)
        assert isinstance(self.temp_s3, TempS3)

    def test_upload_local_s3_is_done(self, source_path, partition_path):
        """This method will test whether the file uploaded successfully in local s3"""
        self.temp_s3 = TempS3(config, logger)
        response = self.temp_s3.upload_local_s3(source_path, partition_path)
        assert response

    @pytest.mark.xfail
    def test_upload_local_s3_is_not_done(self, partition_path):
        """This method will test whether the file is not successfully uploaded in local s3"""
        self.temp_s3 = TempS3(config, logger)
        unavailable_source_path = "D/unavailable"
        response = self.temp_s3.upload_local_s3(unavailable_source_path, partition_path)
        assert not response
