"""This is the test module for test  the load the host aspire files from sftp.
And upload the information as json file with partition based on date in filename"""
import configparser
from datetime import datetime
import boto3
import os
import pytest
from sftp_connection import SftpCon
from load_host_files_sftp_to_s3 import LoadHostFilesSftpToS3, get_partition
from S3.s3 import S3Service
from dummy_S3.dummy_s3 import DummyS3
from dummy_sftp import DummySftp
from moto import mock_s3
from logging_and_download_path import (
    LoggingDownloadpath,
    parent_dir,
)

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
logger_donload = LoggingDownloadpath(config)
logger = logger_donload.set_logger("load_host_aspire_files")


@pytest.fixture
def lpath():
    file_path = os.path.join(
        parent_dir,
        config["local"]["local_file_path"],
        "host_aspire_zip_files",
    )
    return file_path


@pytest.fixture
def wrong_lpath():
    file_path = os.path.join(
        parent_dir,
        config["local"]["local_file_path"],
        "host_aspire_zip",
    )
    return file_path


@pytest.fixture
def file_exist_list():
    s3_obj = S3Service(logger)
    file_list = s3_obj.get_file_list("source/")
    return file_list


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
def partition(file_name):
    date = datetime.strptime(file_name.split(".")[0], "ASP_%Y%m%d").date()
    partition_path = datetime.strftime(date, "pt_year=%Y/pt_month=%m/pt_day=%d/")
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


@pytest.fixture
def prefix():
    return "source"


@pytest.fixture
def wrong_prefix():
    return "wrong_prefix"


# @pytest.fixture
# def s3_test(s3_client, bucket):
#     """This is the fixture for creating s3 bucket"""
#     print(s3_client)
#     res = s3_client.create_bucket(Bucket=bucket)
#     yield


class Test_SftpConnection:
    """This class will check all success and failure test cases in sftp_connection"""

    def test_sftp_connection_object(self):
        """This Method will test for the instance belong to the class SftpCon"""
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

    def test_download_new_file(self, lpath):
        """This method will test the whether the file downloaded successfully"""
        self.sftp_obj = SftpCon()
        result = self.sftp_obj.download_file(lpath, file_exist_list)
        assert os.path.exists(result) is True

    @pytest.mark.xfail
    def test_download_new_file_is_failed(self, lpath):
        """This method will test the whether the file downloaded not successfully"""
        self.sftp_obj = SftpCon()
        result = self.sftp_obj.download_file(lpath, file_exist_list)
        assert os.path.exists(result) is False


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

    def test_get_file_list_done(self, s3_client, s3_test, file, key):
        """This method will test whether get file list successfully"""
        self.my_client = S3Service(logger)
        self.my_client.s3_obj.upload_file(
            file, "msg-practice-induction", "source/" + key
        )
        response = self.my_client.get_file_list("source/")
        assert isinstance(response, list)

    @pytest.mark.xfail
    def test_get_file_list_not_done(self, s3_client, s3_test, file, key):
        """This method will test whether get file list not successful"""
        self.my_client = S3Service(logger)
        self.my_client.s3_obj.upload_file(
            file, "msg-practice-induction", "source/" + key
        )
        response = self.my_client.get_file_list("sourc/")
        assert not response


class Test_LoadHostAspireFiles:
    """This class will test all success and failure cases for
    load_host_aspire_files module"""

    def test_load_host_aspire_files_object(self):
        """This method test the instance belong to the class of LoadHostFilesSftpToS3"""
        self.obj = LoadHostFilesSftpToS3()
        assert isinstance(self.obj, LoadHostFilesSftpToS3)

    def test_download_host_files_done(self):
        """This method will test whether the the file downloaded  successfully"""
        self.obj = LoadHostFilesSftpToS3()
        response = self.obj.download_host_files()
        assert isinstance(response, str)

    @pytest.mark.xfail
    def test_download_host_files_not_done(self):
        """This method will test whether the the file not downloaded  successfully"""
        self.obj = LoadHostFilesSftpToS3()
        response = self.obj.download_host_files()
        assert response is False

    def test_upload_host_files_to_s3_done(self, lpath, bucket_path):
        """This method will test whether upload host files to s3 success"""
        self.obj = LoadHostFilesSftpToS3()
        response = self.obj.upload_host_files_to_s3(lpath, bucket_path)
        assert isinstance(response, str)

    @pytest.mark.xfail
    def test_upload_host_files_to_s3_not_done(self, wrong_lpath, path):
        """This method will test whether upload host files to s3 not success"""
        self.obj = LoadHostFilesSftpToS3()
        response = self.obj.upload_host_files_to_s3(wrong_lpath, path)
        assert response is None

    def test_get_partition_done(self, name, partition):
        """This method will test whether it get partition is sucessful"""
        response = get_partition(name)
        assert response == partition

    @pytest.mark.xfail
    def test_get_partition_path_not_done(
        self,
        wrong_file_name,
    ):
        """This method will test whether it get partition is not successful"""
        response = get_partition(wrong_file_name)
        assert response == None


class Test_dummy_s3:
    """This class will test the dummy_s3 module methods"""

    def test_dummy_s3_obj(self):
        """This method will test the instance belong to the class of DummyS3"""
        self.obj = DummyS3(config, logger)
        assert isinstance(self.obj, DummyS3)

    def test_get_file_list_done(self, prefix):
        """This method will test the whether the
        file list successfully get from dummy s3"""
        self.obj = DummyS3(config, logger)
        response = self.obj.get_file_list(prefix)
        assert isinstance(response, list)

    @pytest.mark.xfail
    def test_get_file_list_not_done(self, wrong_prefix):
        """This method will test the whether the
        file list successfully get from dummy s3"""
        self.obj = DummyS3(config, logger)
        response = self.obj.get_file_list(wrong_prefix)
        assert not response

    def test_upload_dummy_local_s3_done(self, path, file, partition):
        """This method will test whether the upload to dummy s3 is success"""
        self.obj = DummyS3(config, logger)
        dest = path + partition
        response = self.obj.upload_dummy_local_s3(file, dest)
        assert response is True

    @pytest.mark.xfail
    def test_upload_dummy_local_s3_not_done(self, path, partition):
        """This method will test whether the upload to dummy s3 is not success"""
        self.obj = DummyS3(config, logger)
        dest = path + partition
        response = self.obj.upload_dummy_local_s3("unavilable_file", dest)
        print(response)
        assert response is False


class Test_dummy_sftp:
    """This class will test the dummy_s3 module methods"""

    def test_dummy_sftp_obj(self):
        """This method will test the instance belong to the class of DummySftp"""
        self.obj = DummySftp(config, logger)
        assert isinstance(self.obj, DummySftp)

    def test_list_files_done(self):
        """This method will test the whether the
        file list successfully get from dummy sftp"""
        self.obj = DummySftp(config, logger)
        response = self.obj.list_files()
        assert isinstance(response, list)

    @pytest.mark.xfail
    def test_list_files_not_done(self):
        """This method will test the whether the
        file list successfully get from dummy sftp"""
        self.obj = DummySftp(config, logger)
        self.obj.dest_rpath = "unavailable"
        response = self.obj.list_files()
        assert not response

    def test_get_new_file_only_done(self, lpath, prefix):
        """This method will test whether the dummy sftp get new file list is success"""
        self.obj = DummySftp(config, logger)
        self.dummy_s3 = DummyS3(config, logger)
        file_exist_list = self.dummy_s3.get_file_list(prefix)
        response = self.obj.get_new_file_only(lpath, file_exist_list)
        assert response == lpath

    @pytest.mark.xfail
    def test_get_new_file_only_not_done(self, wrong_lpath, prefix):
        """This method will test whether the dummy sftp get new file list is success"""
        self.obj = DummySftp(config, logger)
        self.dummy_s3 = DummyS3(config, logger)
        file_exist_list = self.dummy_s3.get_file_list(prefix)
        response = self.obj.get_new_file_only(wrong_lpath, file_exist_list)
        assert not response
