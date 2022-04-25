"""This is the conftest file which is used for pytest to store fixtures"""

import pytest
from moto import mock_s3
import boto3


@pytest.fixture
def s3_client():
    """This is the fixture for mocking s3 service"""
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        print(conn)
        yield conn


@pytest.fixture
def sftp_client(sftp_server):
    """This is the fixture for mocking sftp server"""
    print(sftp_server.host)
    with sftp_server.client("sample-user") as client:
        sftp = client.open_sftp()
        yield sftp
