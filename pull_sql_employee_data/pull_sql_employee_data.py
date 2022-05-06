"""This module is used to pull the employee information from
the sql server based on date of joining and then upload it to
s3 as json file with partition which created by date of joining"""

import os
from time import time
import pandas as pd
from datetime import datetime, timedelta
from argparse import ArgumentParser
import sys
import configparser
import logging
from sql import SqlConnection
from S3.s3 import S3Service

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

    def __init__(self, s_date, _e_date) -> None:
        """This is the init method for the class of PullSqlEmployeeData"""
        self.s_date = s_date
        self.e_date = _e_date
        self.download_path = os.path.join(parent_dir, config["local"]["local_file_path"], "employee_sql")
        self.sql = SqlConnection(logger)
        self.s3 = S3Service(logger)
    def pull_employee_data_from_sql(self):
        """This method will pull the employee data from sql
        based on given date input and upload to s3"""
        pre_date = datetime.now().date()
        if self.s_date:
            if self.s_date <= pre_date:
                response = self.get_employee_information(self.s_date,self.e_date)
                logging.info("employee information took for start date to end date")
            print(
                "Script ran for start and end date. So script was terminated without update last run.."
            )
            sys.exit()
        response = self.get_employee_information(pre_date,self.e_date)
        print(response)
        return response

    def get_employee_information(self,start,end=datetime.strftime(datetime.now().date(),"%Y-%m-%d")):
        """This method will retrieve the data based on the date passed in query."""
        try:
            query = f"SELECT * FROM Employee  WHERE date_of_join >= '{start}' AND date_of_join < '{end}'"
            response = self.sql.execute_query(query)
            column_names = [i[0] for i in response.description]
            # response  = pd.read_sql(query,self.sql.conn)
            data_frame = pd.DataFrame.from_records(response, columns=column_names)
            
            # print(data_frame)
        except Exception as err:
            response = None
            print(err)
        return response
    
    def create_json_file(self,data_frame,column_names):
        """This method will create the json file for the data frame"""
        try :
            epoch = int(time())
            
            response = pd.DataFrame.to_json(data_frame,
                                            self.download_path+f'/employee_{epoch}.json',
                                            orient='records',lines=True,date_format='iso')
            partition = self.get_partition(data_frame['join_date'])
            
        except Exception as err :
            print(err)
            response = None
        return response
    
    def get_partition(self,join_date):
        """This method will create the partition based on the joining date of employee"""
        try:
            date_obj = datetime.strptime(join_date, "%Y-%m-%d")
            partition_path = date_obj.strftime("pt_year=%Y/pt_month=%m/pt_day=%d/")
        except Exception as err:
            print(err)
            partition_path = None
        return partition_path
    
    def upload_to_s3(self,file,key):
        """This method used to upload the file to s3 which data got sql"""
        response = self.s3.upload_file(file, key)
        return response
    
def get_date(date_str):
    """This is the function it will return the date format from string format"""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


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
    pull_sql_data.pull_employee_data_from_sql()


if __name__ == "__main__":
    main()
