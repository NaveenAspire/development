"""This module will test the all classes and methods
of the task fetch data from public holiday api and upload to s3"""

from public_holiday_api import PublicHoliday
from fetch_public_holiday_data import FetchDataFromPublicHolidayApi
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
logger = logger_donload.set_logger("test_public_holiday_api")

public_holiday_api_section = config["public_holiday_api"]


@pytest.fixture
def args():
    args = {"code": "US", "endpoint": "public_holidays"}
    return args


@pytest.fixture
def wrong_args():
    wrong_args = {
        "code": "AAA",
        "endpoint": "public_holidays",
        "start": 2020,
        "end": 2000,
    }
    return wrong_args


@pytest.fixture
def year():
    return int(date.today().year)


@pytest.fixture
def data_frame(year, args):
    public_holiday = PublicHoliday(config)
    data_frame = public_holiday.get_pubilc_holidays(year, args.get("code"))
    return data_frame


@pytest.fixture
def wrong_country_code():
    return "aaa"


@pytest.fixture
def endpoint():
    return "public_holidays"


@pytest.fixture
def endpoint_with_params():
    return "https://date.nager.at/api/v3/PublicHolidays/2021/US"


@pytest.fixture
def partition_path(year, args):
    partition_path = f"pt_countryCode={args.get('code')}/pt_year={year}/"
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
def path():
    bucket_path = "source"
    return bucket_path


@pytest.fixture
def file_name(args, year):
    file_name = (
        f"{endpoint}_{args.get('code')}_{year}.json"
        if endpoint != "next_public_holidays"
        else f"{endpoint}_{args.get('code')}_{datetime.strftime(year, '%Y%m%d')}.json"
    )
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


class Test_public_holiday_api:
    """This class will test all success and failure cases for public holiday api module"""

    def test_public_holiday_api_object(self):
        """This method will test the instance belongs to the class of PublicHoliday"""
        self.public_holiday = PublicHoliday(config)
        assert isinstance(self.public_holiday, PublicHoliday)

    def test_get_long_weekend_is_done(self, year, args):
        """This method will test whether the
        get long weekend succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        print(type(args))
        response = self.public_holiday.get_long_weekend(year, args.get("code"))
        assert isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_long_weekend_is_not_done(self, year, wrong_country_code):
        """This method will test whether the
        get long weekend not succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_long_weekend(year, wrong_country_code)
        assert response is None

    def test_get_pubilc_holidays_is_done(self, year, args):
        """This method will test whether the
        get public holidays succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_pubilc_holidays(year, args.get("code"))
        assert isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_pubilc_holidays_is_not_done(self, year, wrong_country_code):
        """This method will test whether the
        get public holidays not succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_pubilc_holidays(year, wrong_country_code)
        assert response is None

    def test_get_next_public_holidays_is_done(self, args):
        """This method will test whether the
        get next public holidays succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_next_public_holidays(args.get("code"))
        assert isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_next_public_holidays_is_done(self, wrong_country_code):
        """This method will test whether the
        get next public holidays not succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_next_public_holidays(wrong_country_code)
        assert response is None

    def test_get_available_regions_is_done(self):
        """This method will test whether the
        get available regions is succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_available_regions()
        assert isinstance(response, list)

    @pytest.mark.xfail
    def test_get_available_regions_is_not_done(self):
        """This method will test whether the
        get available regions is not succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_available_regions()
        assert response is None

    def test_get_response_is_done(self, endpoint_with_params):
        """This method will test whether the
        get response regions is succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_response(endpoint_with_params)
        print(type(response))
        assert isinstance(response, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_response_is_not_done(self, endpoint_with_params=None):
        """This method will test whether the
        get response regions is not succesfully done for the enpoint"""
        self.public_holiday = PublicHoliday(config)
        response = self.public_holiday.get_response(endpoint_with_params)
        assert response is None


class Test_FetchDataFromPublicHolidayApi:
    """This test class will test the all
    methods in the class FetchDataFromPublicHolidayApi"""

    def test_fetch_public_holiday_data_object(self):
        """This method will test the instance
        belongs to the class of FetchDataFromPublicHolidayApi"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        assert isinstance(self.fetch_data, FetchDataFromPublicHolidayApi)

    def test_get_data_for_given_year_region_is_done(self, args):
        """This method will test weather get data for given year
        region is successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.get_data_for_given_year_region(args)
        assert response

    @pytest.mark.xfail
    def test_get_data_for_given_year_region_is_not_done(self, wrong_args):
        """This method will test weather get data for given year
        region is not successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            response = self.fetch_data.get_data_for_given_year_region(wrong_args)
            assert pytest_wrapped_e.type == SystemExit
            assert pytest_wrapped_e.value.code == 42

    def test_fetch_public_holidays_data_is_done(self, year, args, endpoint):
        """This method will test weather fetch public holidays data
        is successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.fetch_public_holidays_data(
            year, args.get("code"), endpoint
        )
        assert response

    @pytest.mark.xfail
    def test_fetch_public_holidays_data_is_not_done(self, year, wrong_args, endpoint):
        """This method will test weather fetch public holidays data
        is not successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.fetch_public_holidays_data(
            year, wrong_args.get("code"), endpoint
        )
        assert not response

    def test_fetch_next_public_holidays_is_done(self, args, endpoint):
        """This method will test weather fetch next public holidays
        is successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.fetch_next_public_holidays(
            args.get("code"), endpoint
        )
        assert response

    @pytest.mark.xfail
    def test_fetch_next_public_holidays_is_not_done(self, wrong_args, endpoint):
        """This method will test weather fetch next public holidays
        is not successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.fetch_next_public_holidays(
            wrong_args.get("code"), endpoint
        )
        assert not response

    def test_fetch_long_weekend_is_done(self, year, args, endpoint):
        """This method will test weather fetch long weekend
        is successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.fetch_long_weekend(year, args.get("code"), endpoint)
        assert response

    @pytest.mark.xfail
    def test_fetch_long_weekend_is_not_done(self, year, wrong_args, endpoint):
        """This method will test weather fetch long weekend
        is not successfully done"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.fetch_long_weekend(
            year, wrong_args.get("code"), endpoint
        )
        print(response)
        assert not response

    def test_create_json_is_done(self, args, data_frame, endpoint, year):
        """This method will test whether the json file created successfully."""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.create_json_file(
            data_frame, endpoint, year, args.get("code")
        )
        assert response

    @pytest.mark.xfail
    def test_create_json_file_not_done(self, args, endpoint, year):
        """This method will test whether the json file not created successfully"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        data_frame = None
        response = self.fetch_data.create_json_file(
            data_frame, endpoint, year, args.get("code")
        )
        assert not response

    def test_get_year_partition_is_done(self, args, year):
        """This method will test whether year partition get successfully"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.get_year_partition(year, args.get("code"))
        assert response

    def test_get_date_partition_is_done(self, args, date=datetime.today().date()):
        """This method will test whether date partition get successfully"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        response = self.fetch_data.get_date_partition(date, args.get("code"))
        assert response

    @pytest.mark.xfail
    def test_get_date_partition_is_not_done(self, args):
        """This method will test whether get date partition not successfully"""
        self.fetch_data = FetchDataFromPublicHolidayApi()
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            wrong_date = None
            response = self.fetch_data.get_date_partition(wrong_date, args.get("code"))
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
