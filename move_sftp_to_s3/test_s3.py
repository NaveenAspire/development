"""This is the test module for the task"""

import pytest
from botocore.exceptions import ClientError, ParamValidationError
from s3 import S3Service


class Test_s3:
    """This class test for s3 module"""

    @pytest.fixture
    def bucket_name(self):
        """This is the fixture for get bucket name"""
        return "my-test-bucket"

    @pytest.fixture
    def s3_test(self, s3_client, bucket_name):
        """This is the fixture for creating s3 bucket """
        print(s3_client)
        self.res = s3_client.create_bucket(Bucket=bucket_name)
        yield

    def test_s3_object(self):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Service()
        assert isinstance(self.s3_obj, S3Service)

    def test_upload_file(self, s3_client, s3_test):
        """This method will test file is sucessfully uploaded"""
        try:
            self.my_client = S3Service()
            print(s3_client)
            key = self.my_client.upload_file("sample.csv", "sample.csv")
            response = self.my_client.s3_obj.get_object(Bucket="my-test-bucket", Key=key)
        except ClientError as err:
            print(err)
            response = None
        assert isinstance(response,dict)

    @pytest.mark.xfail
    def test_upload_file_failed(self, s3_client, s3_test):
        """This method will test file not is sucessfully uploaded"""
        try:
            self.my_client = S3Service()
            key = self.my_client.upload_file("<unavailble file name>", "sample.csv")
            print(key)
            response = self.my_client.s3_obj.get_object(Bucket="my-test-bucket", Key=key)
        except (ClientError, ParamValidationError) as err:
            print(err)
            response = None
        assert response is None

    def test_put_object(self, s3_client, s3_test):
        """This method will test object is sucessfully put in s3"""
        try:
            self.my_client = S3Service()
            key = self.my_client.put_object(b"hai hello", "hello.txt")
            response = self.my_client.s3_obj.get_object(Bucket="my-test-bucket", Key=key)
        except ClientError as err:
            print(err)
            response = None
        assert isinstance(response,dict)

    @pytest.mark.xfail
    def test_put_object_failed(self, s3_client, s3_test):
        """This method will test object not is sucessfully put in s3"""
        try:
            self.my_client = S3Service()
            key = self.my_client.put_object("<invalid input>", "file")
            response = self.my_client.s3_obj.get_object(
                Bucket="my-test-bucket", Key=None
            )
        except (ClientError,ParamValidationError) as err:
            print(err)
            response = None
        assert response is None
