import boto3
from moto import mock_dynamodb
from dynamodb import DynamoDB


@mock_dynamodb
class DDV:

    def get_section(self):
        "Test the write_into_table with a valid input data"
        # with mock_dynamodb():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        self.ddb = DynamoDB(dynamodb)
        dcreate = {'AttributeDefinitions':[{'AttributeName': 'section_id','AttributeType': 'N'},{'AttributeName': 'section_name','AttributeType': 'S'},],'TableName':'python_config','KeySchema':[{'AttributeName': 'section_id','KeyType': 'HASH'},{'AttributeName': 'section_name','KeyType': 'RANGE'},],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5},}
        response = self.ddb.create_table(**dcreate)
        dput = {'Item':{'section_id': 1,'section_name': 'thirukkural','section': {'thirukkural_endpoint':'https://api-thirukkural.vercel.app/api','bucket_path':'thirukkural/source/'}}}
        resp = self.ddb.put_item("python_config", **dput)
        dget = {'Key':{'section_id': 1,'section_name': 'thirukkural'}}
        res = self.ddb.get_item("python_config", **dget)
        # print(res["Item"].get('section'))
        
        return res["Item"].get('section')

def main():
    ddv = DDV()
    ddv.get_section()

if __name__ == "__main__":
    main()

