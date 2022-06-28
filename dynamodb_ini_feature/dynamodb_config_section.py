"""This module will used to create the table for the config
file and store section values into the table and use them in script"""

import argparse
import ast
import configparser
from dynamodb import DynamoDB
from logging_and_download_path import LoggingDownloadpath,parent_dir
import os

config = configparser.ConfigParser()
config.read(os.path.join(parent_dir, "develop.ini"))
logger_download = LoggingDownloadpath(config)
logger = logger_download.set_logger("fetch_ebird_api_data")

class DynamodbConfig:
    """This is the class used to create the table for
    config file and store section values as item into the table"""
    
    def __init__(self) -> None:
        """This is the init method for the class DynamodbConfig"""
        self.dynamodb = DynamoDB(config)
    
    def dynamodb_create_table(self,params):
        """This is the method to create the dynamodb table for the user given params"""
        table = self.dynamodb.create_table(params)
        print(table)
        
    def dynamodb_put_item_to_table(self,table_name,params):
        """This is method used to insert the item into the dynamodb table"""
        item_response = self.dynamodb.put_item(table_name,params)
        print(item_response)
        
    def update_item_to_table(self,table_name,params):
        """This method used to update the item into the dynamodb table"""
        update_response = self.dynamodb.update_item(table_name,params)
        print(update_response)


def main():
    """This is the main method for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get user input"
    )
    subparsers = parser.add_subparsers()
    create_table = subparsers.add_parser('create_table')
    create_table.add_argument('params',type=ast.literal_eval)
    put_item = subparsers.add_parser('put_item')
    put_item.add_argument("table",type=str)
    put_item.add_argument("params",type=ast.literal_eval)
    get_item = subparsers.add_parser('get_item')
    get_item.add_argument('table',type=str)
    get_item.add_argument('params',type=ast.literal_eval)
    update_item = subparsers.add_parser('update_item')
    update_item.add_argument('table',type=str)
    update_item.add_argument('params',type=ast.literal_eval)
    args = parser.parse_args()
    

if __name__ == "__main__":
    main()