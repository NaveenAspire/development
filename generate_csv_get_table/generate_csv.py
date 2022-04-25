"""This module that creates a csv file from boto3 glue's get_table() method's response"""
from csv import DictWriter
import argparse
import os
import boto3


class CreateCsv:
    """This is the class which contains the methods for creating csv
    from get_table method response"""

    def __init__(self, database_name):
        """This is the init method for class of CreateCsv"""
        self.database_name = database_name
        self.path = os.path.join(os.getcwd(),'opt/data')
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def create_file(self):
        """This methodis used for create the csv file"""
        response = self.get_tables_response()
        rows = self.get_rows_data(response)
        with open(self.path+"/tables_details.csv", "w", encoding="utf8") as file:
            csv_writer = DictWriter(file, fieldnames=rows[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(rows)

    def get_rows_data(self, table_response):
        """This method used to get list of row for write csv file"""
        row_list = []
        #below table_response["TableList"] is list of dictionaries,
        #each dictionary contains one table information
        for item in table_response["TableList"]:  #item has table details as dict
            if not item["StorageDescriptor"].get("Location"):
                continue
            tmp_dict = {
                "table_name": item["Name"],
                "database_name": item["DatabaseName"],
                "crawler_name": item["Parameters"].get("UPDATED_BY_CRAWLER",""),
                "s3_path": item["StorageDescriptor"]["Location"],
            }
            row_list.append(tmp_dict)
        return row_list

    def get_tables_response(self):
        """This method returns the response of boto3 get_table method"""
        client = boto3.client("glue")
        tables_response = client.get_tables(DatabaseName=self.database_name)
        #table_response as type dictionary
        return tables_response


def main():
    """This is the main method for module of generate_csv"""
    parser = argparse.ArgumentParser(
        description="This argparser is used for get database_name"
    )
    parser.add_argument(
        "--database_name", type=str, help="Enter the database_name", required=True
    )
    args = parser.parse_args()
    create_csv = CreateCsv(args.database_name)
    create_csv.create_file()


if __name__ == "__main__":
    main()
