"""This module will test the all classes and methods
of the task fetch data ebird api and upload to s3"""

from ebird_api import EbirdApi
from fetch_data_from_ebird_api_to_s3 import FetchDataFromEbirdApi
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
logger = logger_donload.set_logger("test_ebird_api")

ebird_api_section = config["ebird_api"]


@pytest.fixture
def region_code():
    return "AU"


@pytest.fixture
def region():
    return "US"


@pytest.fixture
def wrong_region():
    return "AAA"


@pytest.fixture
def date():
    return datetime.strftime(datetime.today().date(), "%Y/%m/%d")


@pytest.fixture
def wrong_region_code():
    return "AAA"


@pytest.fixture
def endpoint(region_code, date):
    endpoint = (
        ebird_api_section.get("historic_observations")
        .replace("<regionCode>", region_code)
        .replace("<date>", date)
    )
    return endpoint


@pytest.fixture
def wrong_endpoint(wrong_region_code, date):
    endpoint = (
        ebird_api_section.get("historic_observations")
        .replace("<regionCode>", wrong_region_code)
        .replace("<date>", date)
    )


@pytest.fixture
def s_date():
    return datetime.strptime("2022-06-01", "%Y-%m-%d").date()


@pytest.fixture
def e_date():
    return datetime.strptime("2022-06-05", "%Y-%m-%d").date()


@pytest.fixture
def endpoint_name():
    return "historic_observations"


@pytest.fixture
def partition_path(endpoint_name, region, date):
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    partition_path = date_obj.strftime(
        f"{endpoint_name}/pt_region={region}/pt_year=%Y/pt_month=%m/pt_day=%d/"
    )
    return partition_path


@pytest.fixture
def data_frame(region_code, date):
    ebird_api = EbirdApi(config)
    data_frame = ebird_api.get_historic_observations(region_code, date)
    return data_frame


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
def file_name(partition_variable):
    return f"{partition_variable.replace('-','')}.json"


# @pytest.fixture
# def file(source_path, file_name):
#     return os.path.join(source_path,file_name)


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


class Test_ebird_api:
    """This class will test all success and failure cases for ebird api module"""

    def test_ebird_api_object(self):
        """This method will test the instance belongs to the class of EbirdApi"""
        self.ebird_api = EbirdApi(config)
        assert isinstance(self.ebird_api, EbirdApi)

    def test_get_historic_obseravations_is_done(self, wrong_region_code):
        """This method will test the get historic observations
        method is successfully get data from endpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_historic_observations(region_code, date)
        assert isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_historic_obseravations_is_not_done(self, wrong_region_code, date):
        """This method will test the get historic observations
        method is not successfully get data from endpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_historic_observations(wrong_region_code, date)
        assert not response

    def test_get_top100_contributors_is_done(self, region_code, date):
        """This method will test the get top 100 contributors
        method is not successfully get data from endpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_top100_contributors(region_code, date)
        assert not response

    @pytest.mark.xfail
    def test_get_top100_contributors_is_done(self, wrong_region_code, date):
        """This method will test the get top 100 contributors
        method is successfully get data from endpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_top100_contributors(wrong_region_code, date)
        assert isinstance(response, pd.DataFrame)

    def test_get_checklist_feed_is_done(self, region_code, date):
        """This method will test weather the get
        checklist feed data is succesfully done for the enpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_checklist_feed(region_code, date)
        return isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_checklist_feed_is_not_done(self, wrong_region_code, date):
        """This method will test weather the get
        checklist feed data is succesfully done for the enpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_checklist_feed(wrong_region_code, date)
        return not response

    def test_get_response_is_done(self, endpoint):
        """This method will test weather the get
        response data is succesfully done for the enpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_response(endpoint)
        return isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_response_is_not_done(self, wrong_endpoint):
        """This method will test weather the get
        response data is not succesfully done for the enpoint"""
        self.ebird_api = EbirdApi(config)
        response = self.ebird_api.get_response(wrong_endpoint)
        return not response


class Test_FetchDataFromEbirdApi:
    """This test class will test the all
    methods in the class FetchDataFromEbirdApi"""

    def test_fetch_data_from_ebird_api_object(self):
        """This method will test the instance
        belongs to the class of FetchDataFromEbirdApi"""
        self.fetch_data = FetchDataFromEbirdApi()
        assert isinstance(self.fetch_data, FetchDataFromEbirdApi)

    def test_fetch_data_for_given_dates_is_done(self, s_date, e_date):
        """This method will test weather successfully
        fetch data for given dates is done"""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.fetch_data_for_given_dates(s_date, e_date)
        assert response

    def test_fetch_data_between_dates_is_done(self, s_date, e_date):
        """This method will test weather successfully
        fetch data between dates is done"""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.fetch_data_between_dates(s_date, e_date)
        assert response

    @pytest.mark.xfail
    def test_fetch_data_between_dates_is_not_done(self, s_date):
        """This method will test weather successfully
        fetch data between dates is not done"""
        self.fetch_data = FetchDataFromEbirdApi()
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            response = self.fetch_data.fetch_data_between_dates(s_date, None)
            assert pytest_wrapped_e.type == SystemExit
            assert pytest_wrapped_e.value.code == 42

    def test_get_response_for_date_is_done(self, s_date):
        """This method will test weather successfully
        get response for date is done"""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.get_response_for_date(s_date)
        assert isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_response_for_date_is_not_done(
        self,
    ):
        """This method will test weather successfully
        get response for date is not done"""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.get_response_for_date(None)
        assert response is None

    def test_add_region_is_done(self, region):
        """This method will test weather successfully
        add region is done"""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.add_region(region)
        assert response

    @pytest.mark.xfail
    def test_add_region_is_not_done(self, wrong_region):
        """This method will test weather successfully
        add region is not done"""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.add_region(wrong_region)
        assert not response

    def test_create_json_is_done(self, data_frame, date, region_code):
        """This method will test weather the json file created successfully."""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.create_json_file(data_frame, date, region_code)
        assert response

    @pytest.mark.xfail
    def test_create_json_file_not_done(self, date, region_code):
        """This method will test weather the json file not created successfully"""
        self.fetch_data = FetchDataFromEbirdApi()
        data_frame = None
        response = self.fetch_data.create_json_file(data_frame, date, region_code)
        assert not response

    def test_get_partition_done(self, region_code, date):
        """This method will test weather partition get successfully"""
        self.fetch_data = FetchDataFromEbirdApi()
        response = self.fetch_data.get_partition(region_code, date)
        assert response

    @pytest.mark.xfail
    def test_get_partition_not_done(self, wrong_region_code):
        """This method will test weather get partition not successfully"""
        self.fetch_data = FetchDataFromEbirdApi()
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            response = self.fetch_data.get_partition(wrong_region_code, None)
            assert pytest_wrapped_e.type == SystemExit
            assert pytest_wrapped_e.value.code == 42


class Test_temp_s3:
    """This class will test all the methods in temp_s3 module"""

    def test_temp_s3_object(self):
        """This method will test the instance belongs to the class of TempS3"""
        self.temp_s3 = TempS3(config, logger)
        assert isinstance(self.temp_s3, TempS3)

    def test_upload_local_s3_is_done(self, source_path, partition_path):
        """This method will test weather the file uploaded successfully in local s3"""
        self.temp_s3 = TempS3(config, logger)
        response = self.temp_s3.upload_local_s3(source_path, partition_path)
        assert response

    @pytest.mark.xfail
    def test_upload_local_s3_is_not_done(self, partition_path):
        """This method will test weather the file is not successfully uploaded in local s3"""
        self.temp_s3 = TempS3(config, logger)
        unavailable_source_path = "D/unavailable"
        response = self.temp_s3.upload_local_s3(unavailable_source_path, partition_path)
        assert not response
