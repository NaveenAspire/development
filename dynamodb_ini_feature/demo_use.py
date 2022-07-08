import boto3
from moto import mock_dynamodb
from dynamodb import DynamoDB
import sys

# import store_data


@mock_dynamodb
class DDV:
    # def __init__(self):
    # self.dynamodb = boto3.resource('dynamodb',region_name='us-east-1')

    # def create_table(self, **kwargs):
    #     """This method will create the table in dynamoDB
    #     Parameter :
    #     """
    #     try:
    #         # print(self.dynamodb)
    #         dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    #         print(dynamodb)
    #         response = dynamodb.create_table(**kwargs)
    #     except Exception as err:
    #         print(err)
    #         sys.exit(f"Table is not created due to {err}")
    #     return response

    # def put_item(self, table_name, **kwargs):
    #     """This module will put item into the table"""
    #     try:
    #         dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    #         table = dynamodb.Table(table_name)
    #         response = table.put_item(**kwargs)
    #     except Exception as err:
    #         print(err)
    #         sys.exit(f"Item is not inserted due to {err}")
    #     return response

    # def get_item(self, table_name, **kwargs):
    #     """This module will get item from the table"""
    #     try:
    #         dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    #         table = dynamodb.Table(table_name)
    #         response = table.get_item(**kwargs)
    #     except ClientError as err:
    #         print(err)
    #         sys.exit(f"Item is not read due to {err}")
    #     return response

    def write_into_table(self):
        "Test the write_into_table with a valid input data"
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        print(dynamodb)
        self.ddb = DynamoDB(dynamodb)
        dcreate = {'AttributeDefinitions':[{'AttributeName': 'section_id','AttributeType': 'N'},{'AttributeName': 'section_name','AttributeType': 'S'},],'TableName':'python_config','KeySchema':[{'AttributeName': 'section_id','KeyType': 'HASH'},{'AttributeName': 'section_name','KeyType': 'RANGE'},],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5},}
        response = self.ddb.create_table(**dcreate)
        dput = {'Item':{'section_id': 1,'section_name': 'thirukkural','section': {'thirukkural_endpoint':'https://api-thirukkural.vercel.app/api','bucket_path':'thirukkural/source/'}}}
        resp = self.ddb.put_item("python_config", **dput)
        dget = {'Key':{'section_id': 1,'section_name': 'thirukkural'}}
        res = self.ddb.get_item("python_config", **dget)
        print(res)
        
        # return #res["Item"].get('section')

        # assert False


# @mock_dynamodb
def dynam():
    with mock_dynamodb():
        conn = boto3.resource("dynamodb", region_name="us-east-1")
        print(conn)
        return conn


dynamodb = dynam()

ddv = DDV()
ddv.write_into_table()
# dcreate = {'TableName':'users','KeySchema':[{'AttributeName': 'username','KeyType': 'HASH'},{'AttributeName': 'last_name','KeyType': 'RANGE'}],'AttributeDefinitions':[{'AttributeName': 'username','AttributeType': 'S'},{'AttributeName': 'last_name','AttributeType': 'S'},],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5}}
# response = ddv.create_table(**dcreate)
# dput = {'Item':{
#         'username': 'janedoe',
#         'first_name': 'Jane',
#         'last_name': 'Doe',
#         'age': 25,
#         'account_type': 'standard_user',
#     }}
# resp = ddv.put_item('users',**dput)
# dget = {'Key':{
#         'username': 'janedoe',
#         'last_name': 'Doe'
#     }}
# res = ddv.get_item('users',**dget)
# print(res)


#     def test_write_into_table(self):
#     "Test the write_into_table with a valid input data"
#     dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
#     print(dynamodb)
#     table_name = 'test'
#     table = dynamodb.create_table(
#     TableName=table_name,
#     KeySchema=[
#         {
#             'AttributeName': 'username',
#             'KeyType': 'HASH'
#         },
#         {
#             'AttributeName': 'last_name',
#             'KeyType': 'RANGE'
#         }
#     ],
#     AttributeDefinitions=[
#         {
#             'AttributeName': 'username',
#             'AttributeType': 'S'
#         },
#         {
#             'AttributeName': 'last_name',
#             'AttributeType': 'S'
#         },
#     ],
#     ProvisionedThroughput={
#         'ReadCapacityUnits': 5,
#         'WriteCapacityUnits': 5
#     }
# )
#     print(table)
#     table.put_item(
# Item={
#         'username': 'janedoe',
#         'first_name': 'Jane',
#         'last_name': 'Doe',
#         'age': 25,
#         'account_type': 'standard_user',
#     }
# )

#     response = table.get_item(
#     Key={
#         'username': 'janedoe',
#         'last_name': 'Doe'
#     }
# )
#     item = response['Item']
#     print(item)

#     assert False
