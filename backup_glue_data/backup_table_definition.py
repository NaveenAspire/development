"""This module that is used for getting tables definition from databases of glue"""
import argparse
import json
import boto3


class BackupTableDefinition:
    """ "This is the class for backup table deatails from databases"""

    def __init__(self, bucket_name) -> None:
        """This is the init method for the class of BackupTableDefinition"""
        self.bucket_name = bucket_name
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")

    def get_database_list(self):
        """This method used for get the databases name in glue"""
        database_list = []
        response = self.glue_client.get_databases().get("DatabaseList")
        for item in response:
            database_list.append(item.get("Name"))
        return database_list

    def get_table_definition(self):
        """This method helpls to get tables definition as json for giving databses"""
        database_list = self.get_database_list()
        for databsase in database_list:
            tables_response_list = self.glue_client.get_tables(DatabaseName=databsase).get(
                "TableList"
            )
            for table_response in tables_response_list:
                key = "glue/table/" + databsase + "/" + table_response["Name"] + ".json"
                data_object = bytes(json.dumps(table_response))
                self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=data_object)


def main():
    """This is the main method for module backup_table_definition"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get bucket name"
    )
    parser.add_argument(
        "--bucket_name", type=str, help="Enter your bucket name", required=True
    )
    args = parser.parse_args()
    backup_table = BackupTableDefinition(args.bucket_name)
    backup_table.get_table_definition()


if __name__ == "__main__":
    main()
