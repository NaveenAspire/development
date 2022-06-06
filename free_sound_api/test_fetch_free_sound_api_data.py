"""This module will test the all classes and methods
of the task fetch data from free sound api and upload to s3"""

import requests
from free_sound_api import FreeSoundApi
import configparser
from logging_and_download_path import LoggingDownloadpath, parent_dir
from datetime import datetime
import pytest
from moto import mock_s3
from S3.s3 import S3Service

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
logger_donload = LoggingDownloadpath(config)
logger = logger_donload.set_logger("test_free_sound_api")


@pytest.fixture
def file_name():
    file_name = "ASP_20220418.zip"
    return file_name


@pytest.fixture
def wrong_file_name():
    file_name = "20220418.zip"
    return file_name


@pytest.fixture
def path():
    bucket_path = "source"
    return bucket_path


@pytest.fixture
def partition(partition_variable):
    date_obj = datetime.strptime(partition_variable, "%Y-%m-%d")
    partition_path = date_obj.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d/")
    return partition_path


@pytest.fixture
def file(file_name, lpath):
    file_name = os.path.join(
        lpath,
        file_name,
    )
    return file_name


@pytest.fixture
def key(partition, file_name):
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

    def test_upload_file_done(self, key, file, s3_client, s3_test):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Service(logger)
        response = self.my_client.upload_file(file, key)
        assert response == key

    @pytest.mark.xfail
    def test_upload_file_not_done(self, key, s3_client, s3_test, file):
        """This method will test file is not sucessfully uploaded"""
        self.my_client = S3Service(logger)
        response = self.my_client.upload_file("file", key)
        assert not response

class Test_free_sound_apoi:
    pass