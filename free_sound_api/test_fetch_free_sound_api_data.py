"""This module will test the all classes and methods
of the task fetch data from free sound api and upload to s3"""

from free_sound_api import FreeSoundApi, pagination
from fetch_free_sound_data_to_s3 import FetchDataFromFreeSoundApi
import configparser
from logging_and_download_path import LoggingDownloadpath, parent_dir
from datetime import datetime
import pytest
from moto import mock_s3
from cryptography.fernet import Fernet
import pandas as pd
import boto3
import os
from Temp_s3.temp_s3 import TempS3
from S3.s3 import S3Service

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
logger_donload = LoggingDownloadpath(config)
logger = logger_donload.set_logger("test_free_sound_api")

free_sound_api_section = config["free_sound_api"]


@pytest.fixture
def wrong_file_name():
    file_name = "20220418.zip"
    return file_name


@pytest.fixture
def partition_variable():
    return "2022-05-02"


@pytest.fixture
def wrong_partition_variable():
    return "2022-05-02T"


@pytest.fixture
def sound_id():
    return "636480"


@pytest.fixture
def wrong_sound_id():
    return "1223"


@pytest.fixture
def username():
    return "Jovica"


@pytest.fixture
def wrong_username():
    return "aaaa"


@pytest.fixture
def similar_sound_endpoint():
    return free_sound_api_section.get("similar_sound")


@pytest.fixture
def user_packs_endpoint():
    return free_sound_api_section.get("user_packs")


@pytest.fixture
def data_frame_response(username):
    free_sound_api = FreeSoundApi(config)
    data_frame = free_sound_api.user_packs(username)
    return data_frame


@pytest.fixture
def params():
    fernet_key = free_sound_api_section.get("fernet_key")
    fernet = Fernet(fernet_key)
    encrypted_key = bytes(free_sound_api_section.get("access_token"), "utf-8")
    access_key = fernet.decrypt(encrypted_key).decode()
    params = {
        "token": access_key,
        "page_size": 150,
    }
    return params


@pytest.fixture
def partition_path(partition_variable):
    date_obj = datetime.strptime(partition_variable, "%Y-%m-%d")
    partition_path = date_obj.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d/")
    return partition_path


@pytest.fixture
def download_path():
    log_and_download = LoggingDownloadpath(config)
    path = log_and_download.set_downloadpath("test_free_sound_api")
    return path


@pytest.fixture
def source_path(data_frame_response, file_name, download_path):
    data_frame_response.to_json(
        os.path.join(download_path, file_name),
        orient="records",
        lines=True,
    )
    file_path = os.path.join(download_path, file_name)
    return file_path


@pytest.fixture
def wrong_params():
    params = {
        "token": "access_key",
        "page_size": 150,
    }
    return params


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
def file_name(partition_variable):
    return f"{partition_variable.replace('-','')}.json"

@pytest.fixture
def file(source_path, file_name):
    return os.path.join(source_path,file_name)

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


class Test_free_sound_api:
    """This class will test all success and failure cases for free sound api module"""

    def test_free_sound_api_object(self):
        """This method will test the instance belongs to the class of FreeSoundApi"""
        self.free_sound_api = FreeSoundApi(config)
        assert isinstance(self.free_sound_api, FreeSoundApi)

    def test_similar_sounds_is_done(self, sound_id):
        """This method will test the similar sound
        method is successfully get data from endpoint"""
        self.free_sound_api = FreeSoundApi(config)
        response = self.free_sound_api.similar_sounds(sound_id)
        assert isinstance(response, pd.DataFrame)

    def test_similar_sounds_is_not_done(self, wrong_sound_id):
        """This method will test the similar sound
        method is not successfully get data from endpoint"""
        self.free_sound_api = FreeSoundApi(config)
        response = self.free_sound_api.similar_sounds(wrong_sound_id)
        assert not response

    def test_user_packs_is_done(self, username):
        """This method will test the user packs
        method is successfully get data from endpoint"""
        self.free_sound_api = FreeSoundApi(config)
        response = self.free_sound_api.user_packs(username)
        assert isinstance(response, pd.DataFrame)

    def test_user_packs_is_not_done(self, wrong_username):
        """This method will test the user packs
        method is not successfully get data from endpoint"""
        self.free_sound_api = FreeSoundApi(config)
        response = self.free_sound_api.user_packs(wrong_username)
        assert not response

    def test_pagination_is_done(self, similar_sound_endpoint,params):
        """This method will test weather the
        pagination succesfully done for the enpoint"""
        self.free_sound_api = FreeSoundApi(config)
        response = pagination(similar_sound_endpoint, params)
        return isinstance(response, pd.DataFrame)

    def test_pagination_is_not_done(self, similar_sound_endpoint, wrong_params):
        """This method will test weather the
        pagination succesfully done for the enpoint"""
        self.free_sound_api = FreeSoundApi(config)
        response = pagination(similar_sound_endpoint, wrong_params)
        return not response


class Test_FetchDataFromFreeSoundApi:
    """This test class will test the all
    methods in the class FetchDataFromFreeSoundApi"""

    def test_fetch_data_from_free_sound_object(self):
        """This method will test the instance
        belongs to the class of FetchDataFromFreeSoundApi"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        assert isinstance(self.fetch_data, FetchDataFromFreeSoundApi)

    def test_fetch_similar_sounds_is_done(self, sound_id):
        """This method will test weather successfully
        fetch similar sounds for the given sound id"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        response = self.fetch_data.fetch_similar_sounds(sound_id)
        assert response

    def test_fetch_similar_sounds_is_not_done(self, wrong_sound_id):
        """This method will test weather not
        successfully fetch similar sounds for the given sound_id"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        response = self.fetch_data.fetch_similar_sounds(wrong_sound_id)
        assert not response

    def test_fetch_user_packs_is_done(self, username):
        """This method will test weather successfully fetch user_packs is done"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        response = self.fetch_data.fetch_user_packs(username)
        assert response

    def test_fetch_user_packs_is_not_done(self, wrong_username):
        """This method will test weather not successfully fetch user_packs done"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            response = self.fetch_data.fetch_user_packs(wrong_username)
            assert pytest_wrapped_e.type == SystemExit
            assert pytest_wrapped_e.value.code == 42


    def test_create_json_is_done(self, data_frame_response, file_name):
        """This method will test weather the json file created successfully."""
        self.fetch_data = FetchDataFromFreeSoundApi()
        response = self.fetch_data.create_json_file(data_frame_response, file_name)
        assert response

    def test_create_json_file_not_done(self, data_frame_response):
        """This method will test weather the json file not created successfully"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        file_name = None
        response = self.fetch_data.create_json_file(data_frame_response, file_name)
        assert not response

    def test_get_partition_done(self, partition_variable):
        """This method will test weather partition get successfully"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        response = self.fetch_data.get_partition(partition_variable)
        assert response

    def test_get_partition_not_done(self, wrong_partition_variable):
        """This method will test weather get partition not successfully"""
        self.fetch_data = FetchDataFromFreeSoundApi()
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            response = self.fetch_data.get_partition(wrong_partition_variable)
            assert pytest_wrapped_e.type == SystemExit
            assert pytest_wrapped_e.value.code == 42


class Test_temp_s3:
    """This class will test all the methods in temp_s3 module"""

    def test_temp_s3_object(self):
        """This method will test the instance belongs to the class of TempS3"""
        self.temp_s3 = TempS3(config, logger)
        assert isinstance(self.temp_s3, TempS3)

    def test_upload_local_s3_is_done(self,source_path,partition_path):
        """This method will test weather the file uploaded successfully in local s3"""
        self.temp_s3 = TempS3(config, logger)
        response = self.temp_s3.upload_local_s3(source_path, partition_path)
        assert response

    def test_upload_local_s3_is_not_done(self):
        """This method will test weather the file is not successfully uploaded in local s3"""
        self.temp_s3 = TempS3(config, logger)
        unavailable_source_path = "D/unavailable"
        response = self.temp_s3.upload_local_s3(unavailable_source_path, partition_path)
        assert not response
