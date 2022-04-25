"""This module that is used for getting crawler definition from  glue"""
import argparse
import json
import boto3


class BackupCrawlerDefinition:
    """ "This is the class for backup crawler deatails from glue"""

    def __init__(self, bucket_name) -> None:
        """This is the init method for the class of BackupCrawlerDefinition"""
        self.bucket_name = bucket_name
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")

    def get_crawlers_definition(self):
        """This method helpls to get tables definition as json for giving databses"""
        response = self.glue_client.get_crawlers()
        for crawler in response["Crawlers"]:
            key = "glue/crawler/" + crawler["Name"] + ".json"
            data_object = bytes(json.dumps(crawler))
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=data_object)


def main():
    """This is the main function for the module of backup_crawler_definition"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get bucket name"
    )
    parser.add_argument(
        "--bucket_name", type=str, help="Enter your bucket name", required=True
    )
    args = parser.parse_args()
    backup_table = BackupCrawlerDefinition(args.bucket_name)
    backup_table.get_crawlers_definition()


if __name__ == "__main__":
    main()
