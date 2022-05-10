"""This module is used to pull the employee information from
the sql server based on date of joining and then upload it to
s3 as json file with partition which created by date of joining"""
import os
from time import time
import sys
import configparser
import logging
from datetime import datetime, timedelta
from argparse import ArgumentParser
import pandas as pd
from sql import SqlConnection
from S3.s3 import S3Service
from dummy_S3.dummy_s3 import DummyS3

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")

log_dir = os.path.join(parent_dir, config["local"]["local_log"], "sql_employee_data")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "employee_data.log")

logging.basicConfig(
    filename=log_file,
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s : %(lineno)d - %(message)s",
    level=logging.INFO,
    force=True,
)
logger = logging.getLogger()


class PullSqlEmployeeData:
    """This is the class that contains methods for the pull the
    employee data from sql and upload to s3 as json with partition"""

    def __init__(self, s_date, _e_date, con_param, exclude) -> None:
        """This is the init method for the class of PullSqlEmployeeData"""
        self.s_date = s_date
        self.e_date = _e_date
        self.con_param = con_param
        self.exclude = exclude
        self.last_run = get_date(config["pull_sql_employee_data"]["script_run"])
        self.download_path = os.path.join(
            parent_dir, config["local"]["local_file_path"], "employee_sql"
        )
        os.makedirs(self.download_path, exist_ok=True)
        self.sql = SqlConnection(logger)
        self.s3 = S3Service(logger)
        self.dummy_s3 = DummyS3(config, logger)

    def pull_employee_data_from_sql(self):
        """This method will pull the employee data from sql
        based on given date input and upload to s3"""
        pre_date = datetime.now().date() - timedelta(1)
        if self.s_date:
            if self.s_date <= pre_date:
                response = self.get_employee_information(self.s_date, self.e_date)
                logging.info("employee information took for start date to end date")
            print(
                "Script ran for start and end date."
                + "So script was terminated without update last run.."
            )
            sys.exit()
        elif self.last_run <= pre_date:
            response = self.get_employee_information(pre_date, self.e_date)
        else:
            print("Data available upto date...")
            response = None
        return response

    def get_employee_information(
        self, start, end=datetime.strftime(datetime.now().date(), "%Y-%m-%d")
    ):
        """This method will retrieve the data based on the date passed in query."""
        try:
            query = (
                "SELECT * FROM Employee  WHERE date_of_join >="
                f"'{start}' AND date_of_join < '{end}'"
            )
            data_frame = self.sql.read_query(query)
            print(data_frame)
            while start < end:
                self.create_json_file(data_frame, start)
                start = start + timedelta(1)
        except Exception as err:
            data_frame = None
            print(err)
        return data_frame

    def get_employee_data_where(self):
        """This method will pass the date and condition to where query
        and get employee data based on paased values"""
        try:
            if not self.e_date and self.s_date == self.last_run-timedelta(1):
                print("Data available upto date..")
                sys.exit()
            data_frame = self.sql.where_query(
                "Employee",
                "date_of_join",
                self.s_date,
                end=self.e_date,
                con_param=self.con_param,
                exclude=self.exclude,
            )
            start = min(data_frame["date_of_join"])
            while start <= max(data_frame["date_of_join"]):
                self.create_json_file(data_frame, start)
                start = start + timedelta(1)
        except Exception as err:
            data_frame = None
            print(err)
        return data_frame

    def create_json_file(self, data_frame, date):
        """This method will create the json file for the data frame"""
        try:
            epoch = int(time())
            str_date = datetime.strftime(date, "%Y-%m-%d")
            new_df = data_frame[(data_frame.date_of_join == date)]
            response = None
            if not new_df.empty:
                new_df = new_df.astype({"date_of_birth": str, "date_of_join": str})
                response = pd.DataFrame.to_json(
                    new_df,
                    self.download_path + f"/employee_{epoch}.json",
                    orient="records",
                    lines=True,
                )
                key = self.get_partition(str_date) + f"employee_{epoch}.json"
                # self.s3.upload_file(self.download_path+f'/employee_{epoch}.json', key)
                self.dummy_s3.upload_dummy_local_s3(
                    self.download_path + f"/employee_{epoch}.json",
                    "sql_employee/" + self.get_partition(str_date),
                )
        except Exception as err:
            print(err)

            response = None
        return response

    def get_partition(self, join_date):
        """This method will create the partition based on the joining date of employee"""
        try:
            date_obj = datetime.strptime(str(join_date), "%Y-%m-%d")
            partition_path = date_obj.strftime("pt_year=%Y/pt_month=%m/pt_day=%d/")
        except Exception as err:
            print(err)
            partition_path = None
        return partition_path

    def upload_to_s3(self, file, key):
        """This method used to upload the file to s3 which data got sql"""
        response = self.s3.upload_file(file, key)
        return response


def get_date(date_str):
    """This is the function it will return the date format from string format"""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def set_last_run():
    """This function that will set the last run of the script"""
    config.set("pull_sql_employee_data", "script_run", str(datetime.now().date()))
    with open(parent_dir + "/develop.ini", "w", encoding="utf-8") as file:
        config.write(file)

def get_bool(bool_str):
    """This is the function it will return the bool format from string format"""
    if bool_str not in {'False','True'}:
        raise ValueError('Not a valid boolean string')
    return True if bool_str=='True' else False


def main():
    """This the main method for the module pull_sql_employee_data"""
    parser = ArgumentParser(description="Argparser used for getting input from user")
    parser.add_argument(
        "--s_date",
        type=get_date,
        help="Enter start date for pull data",
        default=datetime.now().date() - timedelta(1),
    )
    parser.add_argument(
        "--e_date",
        type=get_date,
        help="Enter end date for pull data",
        # default=datetime.now().date(),
    )
    parser.add_argument(
        "--con_param", type=str, help="Enter where condition for pull data", default="="
    )
    parser.add_argument(
        "--exclude", type=get_bool, help="Enter bool  for between end exclude or not", default=False
    )
    args = parser.parse_args()
    pull_sql_data = PullSqlEmployeeData(args.s_date, args.e_date, args.con_param, args.exclude)
    # pull_sql_data.pull_employee_data_from_sql()
    pull_sql_data.get_employee_data_where()


if __name__ == "__main__":
    main()
    # set_last_run()
