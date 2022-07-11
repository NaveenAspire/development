"""This module will test the dynamodb operations by using pytest."""
from ctypes.wintypes import HENHMETAFILE
import os
from dynamodb import DynamoDB
import boto3
from moto import mock_dynamodb
import pytest

@pytest.fixture
def dynamo_resource():
    """This is the fixture for mocking s3 service"""
    with mock_dynamodb():
        conn = boto3.client("s3", region_name="us-east-1")
        print(conn)
        yield conn

class Test_Dynamodb:
    """This class will have the test methods for test all the dynamodb operations."""

    def test_create_table_is_done(self,dynamo_resource,create_table_input):
        """This method will test the create table operation in dynamodb is done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(create_table_input)
        assert response

    @pytest.fixture
    def test_create_table_is_not_done(self,dynamo_resource,wrong_create_table_input):
        """This method will test the create table operation in dynamodb is not done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(wrong_create_table_input)
        assert response

    def test_put_item_is_done(self,dynamo_resource,put_item_input):
        """This method will test the create table operation in dynamodb is done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(put_item_input)
        assert response

    @pytest.fixture
    def test_put_item_is_not_done(self,dynamo_resource,wrong_put_item_input):
        """This method will test the create table operation in dynamodb is not done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(wrong_put_item_input)
        assert response

    def test_get_item_is_done(self,dynamo_resource,get_item_input):
        """This method will test the create table operation in dynamodb is done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(get_item_input)
        assert response

    @pytest.fixture
    def test_get_item_is_not_done(self,dynamo_resource,wrong_get_item_input):
        """This method will test the create table operation in dynamodb is not done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(wrong_get_item_input)
        assert response

    def test_update_item_is_done(self,dynamo_resource,update_item_input):
        """This method will test the create table operation in dynamodb is done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(update_item_input)
        assert response

    @pytest.fixture
    def test_update_item_is_not_done(self,dynamo_resource,wrong_update_item_input):
        """This method will test the create table operation in dynamodb is not done"""
        self.dynamodb_obj = DynamoDB(dynamo_resource)
        response = self.dynamodb_obj.create_table(wrong_update_item_input)
        assert response