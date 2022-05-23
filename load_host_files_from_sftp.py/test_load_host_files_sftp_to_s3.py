"""This is the test module for test  the load the host aspire files from sftp.
And upload the information as json file with partition based on date in filename"""
import configparser
from datetime import date
import os
import pytest
import pandas as pd
from load_host_files_sftp_to_s3 import LoadHostFilesSftpToS3
from S3.s3 import S3Service
from logging_and_download_path import (
    LoggingDownloadpath,
    parent_dir,
)
from move_sftp_to_s3.sftp_connection import SftpCon
from nobel_prizes_laureates.fetch_nobelprize_and_laureates import get_partition
config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
logger_donload = LoggingDownloadpath(config)
logger = logger_donload.set_logger('nobel_prizes_laureates')

@pytest.fixture
def lpath():
    file_path = os.path.join(
        parent_dir,
        config["local"]["local_file_path"],
        "host_aspire_zip_files",)
    return file_path

@pytest.fixture
def file_exist_list():
    s3_obj = S3Service(logger)
    file_list = s3_obj.get_file_list('source/')
    return file_list

@pytest.fixture
def partition(year):
    partition_path = f"pt_year={year}"
    return partition_path


@pytest.fixture
def file_name():
    file_name = "ASP_20220418.zip"
    return file_name


@pytest.fixture
def file(file_name):
    file_name = os.path.join(file_name,file)
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


class Test_SftpConnection:
    """This class will check all success and failure test cases in nobelPrize api"""

    def test_sftp_connection_object(self):
        """This Method will test for the instance belong to the class MetaWeatherApi"""
        self.obj = SftpCon(config)
        assert isinstance(self.obj, SftpCon)

    def test_list_files(self):
        """This method test weather list_sftp_files return files or not"""
        sftp_obj = SftpCon()
        result = sftp_obj.list_files()
        assert type(result) == list

    @pytest.mark.xfail
    def test_list_files_failed(self):
        """This method test weather list_sftp_files return files or not"""
        sftp_obj = SftpCon()
        result = sftp_obj.list_files()
        assert result == None

    def test_download_new_file(self,lpath):
        """This method will test the when the file downloaded or not"""
        self.sftp_obj = SftpCon()
        result = self.sftp_obj.download_file(lpath,file_exist_list)
        assert os.path.exists(result) == True

    @pytest.mark.xfail
    def test_download_new_file_is_failed(self,lpath):
        """This method will test the when the file downloaded or not"""
        self.sftp_obj = SftpCon()
        result = self.sftp_obj.download_file(lpath,file_exist_list)
        assert os.path.exists(result) == False


class Test_s3:
    """This class will test all success and failure cases for s3 module"""

    def test_s3_object(self):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Service(logger)
        assert isinstance(self.s3_obj, S3Service)

    def test_upload_file(self, partition, key, bucket, bucket_path, file):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Service(logger)
        self.my_client.upload_file(file, key)
        self.files = self.my_client.s3_obj.list_files(
            bucket, bucket_path + "/" + partition
        )
        assert len(self.files) > 0

    @pytest.mark.xfail
    def test_upload_file_failed(
        self, key, bucket, bucket_path, partition, unavailable_file
    ):
        """This method will test file is not sucessfully uploaded"""
        self.my_client = S3Service(logger)
        self.my_client.upload_file(unavailable_file, key)
        self.files = self.my_client.s3_obj.list_files(
            bucket, bucket_path + "/" + partition
        )
        assert len(self.files) == 0


class Test_LoadHostAspireFiles:
    """This class will test all success and failure cases for
    fetch_nobel_prizes_laureates module"""

    def test_nobel_prizes_laureates_object(self, year, year_to):
        """This method test the instance belong to the class of NobelprizeLaureates"""
        self.obj = LoadHostFilesSftpToS3()
        assert isinstance(self.obj, LoadHostFilesSftpToS3)

    def test_download_host_files_done(self):
        """This method will test whether the reponse get successfully"""
        self.obj = LoadHostFilesSftpToS3()
        response = self.obj.download_host_files()
        assert isinstance(response, str)

    @pytest.mark.xfail
    def test_download_host_files_not_done(self):
        """This method will test whether the reponse not get successfully"""
        self.obj = LoadHostFilesSftpToS3()
        response = self.obj.download_host_files()
        assert response is False

    def test_upload_host_files_to_s3_done(self,lpath,bucket_path):
        """This method will test whether response get successfully for given endpoint"""
        self.obj = LoadHostFilesSftpToS3()
        response = self.obj.upload_host_files_to_s3(lpath,bucket_path)
        assert isinstance(response, str)

    @pytest.mark.xfail
    def test_upload_host_files_to_s3_not_done(self,):
        """This method will test whether response get successfully for given endpoint"""
        self.obj = LoadHostFilesSftpToS3()
        lapth = "unavilable_pah"
        response = self.obj.upload_host_files_to_s3(lapth,bucket_path)
        assert isinstance(response, str)


    def test_get_partition_done(self, name, partition):
        """This method will test whether it get paartition is sucessful"""
        response = get_partition(name)
        assert response == partition

    @pytest.mark.xfail
    def test_get_partition_path_not_done(self,name,partition):
        """This method will test whether it get partition is sucessful"""
        response = get_partition(name)
        assert response == partition
    