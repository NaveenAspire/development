"""This module will test the all classes and methods
of the task fetch data from metropoilitan museum api and upload to s3"""

from metroploitan_mesuem_api import MetropolitanMesuem
from fetch_metropolitan_museum_data import FetchMetropolitanMuseumData
import configparser
from logging_and_download_path import LoggingDownloadpath, parent_dir
from datetime import date, datetime
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
logger = logger_donload.set_logger("test_metropolitan_museum_api")

metropolitan_museum_section = config["metropolitan_museum"]


@pytest.fixture
def object_id():
    return 1


@pytest.fixture
def wrong_object_id():
    return 1.1


@pytest.fixture
def data_frame(object_id):
    metropolitan_museum = MetropolitanMesuem(config)
    data_frame = metropolitan_museum.get_object_details(object_id)
    return data_frame


@pytest.fixture
def partition_path(object_id):
    partition_path = f"pt_id={object_id}"
    return partition_path


@pytest.fixture
def download_path():
    log_and_download = LoggingDownloadpath(config)
    path = log_and_download.set_downloadpath("test_metropolitan_museum_api")
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
def path():
    bucket_path = "source"
    return bucket_path


@pytest.fixture
def file_name(object_id):
    file_name = f"{object_id}.json"
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


class Test_MetropolitanMesuem:
    """This class will test all success and failure cases for metropolitan museum api module"""

    def test_metropolitan_mesuem_api_object(self):
        """This method will test the instance belongs to the class of MetropolitanMesuem"""
        self.metropolitan_museum = MetropolitanMesuem(config)
        assert isinstance(self.metropolitan_museum, MetropolitanMesuem)

    def test_get_object_details_is_done(self, object_id):
        """This method will test whether the
        get object deatils succesfully done for the enpoint"""
        self.metropolitan_museum = MetropolitanMesuem(config)
        response = self.metropolitan_museum.get_object_details(object_id)
        assert isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_object_details_is_not_done(self, wrong_object_id):
        """This method will test whether the
        get object deatils is not succesfully done for the enpoint"""
        self.metropolitan_museum = MetropolitanMesuem(config)
        response = self.metropolitan_museum.get_object_details(wrong_object_id)
        assert response is None


class Test_FetchMetropolitanMuseumData:
    """This test class will test the all
    methods in the class Test_FetchMetropolitanMuseumData"""

    def test_fetch_metropolitan_musuem_object(self):
        """This method will test the instance
        belongs to the class of Test_FetchMetropolitanMuseumData"""
        self.fetch_data = FetchMetropolitanMuseumData()
        assert isinstance(self.fetch_data, FetchMetropolitanMuseumData)

    def test_fetch_object_details_is_done(self, object_id):
        """This method will test weather fetch object details is successfully done"""
        self.fetch_data = FetchMetropolitanMuseumData()
        response = self.fetch_data.fetch_object_details(object_id)
        assert response

    @pytest.mark.xfail
    def test_fetch_object_details_is_not_done(self, wrong_object_id):
        """This method will test weather fetch object details is not successfully done"""
        self.fetch_data = FetchMetropolitanMuseumData()
        response = self.fetch_data.fetch_object_details(wrong_object_id)
        assert not response

    def test_create_json_is_done(self, data_frame, object_id):
        """This method will test whether the json file created successfully."""
        self.fetch_data = FetchMetropolitanMuseumData()
        response = self.fetch_data.create_json_file(data_frame, object_id)
        assert response

    @pytest.mark.xfail
    def test_create_json_file_not_done(self, object_id):
        """This method will test whether the json file not created successfully"""
        self.fetch_data = FetchMetropolitanMuseumData()
        data_frame = None
        response = self.fetch_data.create_json_file(data_frame, object_id)
        assert not response

    def test_get_partition_is_done(self, object_id):
        """This method will test whether year partition get successfully"""
        self.fetch_data = FetchMetropolitanMuseumData()
        response = self.fetch_data.get_partition(object_id)
        assert response


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
