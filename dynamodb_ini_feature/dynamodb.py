"""This module will used to connect the dynamoDB with
python using boto3 and performs operations for dynamoDB"""

import sys
import boto3
from botocore.exceptions import ClientError

class DynamoDB:
    """This used to connect DynamoDB with python
    boto3 and has method for dynamoDB operations"""
    
    def __init__(self, config_obj) -> None:
        """This is the init method for the class DynamoDB
        Parameter :
            config_obj = ConfigParser object"""
        self.config = config_obj
        self.section = config_obj["dynamoDB"]
        self.dynamodb = boto3.resource('dynamodb')
    
    def create_table(self,**kwargs):
        """This method will create the table in dynamoDB
        Parameter :
            """
        try :
            response = self.dynamodb.create_table(**kwargs)
        except Exception as err :
            print(err)
            sys.exit(f"Table is not created due to {err}")
        return response
    
    def put_item(self,table_name,**kwargs):
        """This module will put item into the table"""
        try : 
            table = self.dynamodb.Table(table_name)
            response = table.put_item(**kwargs)
        except Exception as err :
            print(err)
            sys.exit(f"Item is not inserted due to {err}")
        return response
    
    def get_item(self,table_name,**kwargs):
        """This module will get item from the table"""
        try : 
            table = self.dynamodb.Table(table_name)
            response = table.get_item(**kwargs)
        except ClientError as err:
            print(err)
            sys.exit(f"Item is not read due to {err}")
        return response['Item']

    def update_item(self,table_name,**kwargs):
        """This module will get item from the table"""
        try : 
            table = self.dynamodb.Table(table_name)
            response = table.update(**kwargs)
        except ClientError as err:
            print(err)
            sys.exit(f"Item is not read due to {err}")
        return response['Item']
        