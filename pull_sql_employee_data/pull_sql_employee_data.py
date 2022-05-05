"""This module is used to pull the employee information from
the sql server based on date of joining and then upload it to
s3 as json file with partition which created by date of joining"""

import os
import pandas as pd
from datetime import datetime, timedelta
from argparse import ArgumentParser
import sys
import configparser
import logging
from sql import SqlConnection

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
    level=logging.info,
    force=True,
)
logger = logging.getLogger()


class PullSqlEmployeeData:
    """This is the class that contains methods for the pull the
    employee data from sql and upload to s3 as json with partition"""

    def __init__(self, s_date, _e_date) -> None:
        """This is the init method for the class of PullSqlEmployeeData"""
        self.s_date = s_date
        self.e_date = _e_date
        self.last_run = ""
        self.sql = SqlConnection(logger)

    def pull_employee_data_from_sql(self):
        """This method will pull the employee data from sql
        based on given date input and upload to s3"""
        curr_date = datetime.now().date()
        if self.s_date:
            if self.s_date < curr_date:
                response = self.get_employee_information_between_two_dates(self.s_date, self.e_date)
                logging.info("employee information took for start date to end date")
            print(
                "Script ran for start and end date. So script was terminated without update last run.."
            )
            sys.exit()
        elif self.last_run and self.last_run < curr_date:
            print(self.last_run)
            response = self.get_employee_information_between_two_dates(self.last_run, curr_date)
            logging.info("employee information took for last run date to yesterday's date")
        else:
            data_exist = "Data available upto date..."
            self.get_employee_information(curr_date - timedelta(1)) if not self.last_run else print(
                data_exist
            )
            response = None
            logging.info("employee information took for yesterday's date")
        return response

    def get_employee_information_between_two_dates(self, start, end):
        """This method will get the employee information between two dates"""
        try:
            dates = []
            while start < end and start < datetime.now().date() - timedelta(1):
                self.get_employee_information(start)
                logging.info(f"employee information successfully took for {start}")
                dates.append(start)
                start = start + timedelta(1)
        except Exception as err:
            print(err)
            dates = None
        return dates

    def get_employee_information(self, doj):
        try:
            query = ""
            self.sql.retrieve(query)
        except Exception as err:
            response = None
        return response


def get_date(date_str):
    """This is the function it will return the date format from string format"""
    return datetime.strptime(date_str, "%d-%m-%Y")


def main():
    """This the main method for the module pull_sql_employee_data"""
    parser = ArgumentParser(description="Argparser used for getting input from user")
    parser.add_argument(
        "--s_date",
        type=get_date,
        help="Enter start date for pull data",
    )
    parser.add_argument(
        "--e_date",
        type=get_date,
        help="Enter end date for pull data",
        default=datetime.now().date() - timedelta(1),
    )
    args = parser.parse_args()
    pull_sql_data = PullSqlEmployeeData(args.s_date, args.e_date)


if __name__ == "__main__":
    main()
