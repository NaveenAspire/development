"""This is the test module for test  the pull data employee information from sql server based on joining date.
And upload the information as json file with partition with joining data"""
import configparser
import os
import logging
import pytest
from time import time
import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime, timedelta
from sqlalchemy.engine import URL,create_engine
from sql import SqlConnection
from S3.s3 import S3Service
from pull_sql_employee_data import PullSqlEmployeeData, get_bool, get_date,parent_dir, set_last_run


log_dir = os.path.join(parent_dir, "opt/logging/sql_employee_data/")
os.makedirs(log_dir,exist_ok=True)
log_file = os.path.join(log_dir, "test_employee_information.log")
logging.basicConfig(
    filename=log_file,
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    force=True,
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")

@pytest.fixture
def s_date():
    str_date = "2022-05-01"
    start_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return start_date

@pytest.fixture
def e_date():
    str_date = "2022-05-03"
    end_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return end_date

@pytest.fixture
def s_date_none():
    return None

@pytest.fixture
def e_date_none():
    return None

@pytest.fixture
def table():
    return "Employee"

@pytest.fixture
def column():
    return "date_of_join"

@pytest.fixture
def con_param():
    return "="

@pytest.fixture
def exclude():
    return False

@pytest.fixture
def connection_string():
    return config["sql"]["connection_string"]

@pytest.fixture
def wrong_connection_string():
    return None

@pytest.fixture
def sql_con(connection_string):
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    conn = create_engine(connection_url)
    return conn

@pytest.fixture
def query(table,column,con_param,s_date,e_date_none,exclude):
    if not e_date_none and con_param in ['=','<','>','<=','>='] :
        query_str = f"SELECT * FROM {table}  WHERE {column} {con_param}"\
                f"'{s_date}'"
    elif e_date_none and con_param == 'BETWEEN' :
        e_date_none = e_date_none - timedelta(1) if exclude else e_date_none
        query_str = f"SELECT * FROM {table}  WHERE {column} "\
                f"{con_param}'{s_date}' AND '{e_date_none}'"
    return query_str

@pytest.fixture
def data_frame(query,sql_con):
    try:
        df_list =[]
        for chunk in pd.read_sql(query,sql_con,chunksize=1000):
            df_list.append(chunk)
        data_frame = pd.concat(df_list)
    except Exception as err:
        print(err)
        data_frame = None
    return data_frame

@pytest.fixture
def empty_data_frame():
    return pd.DataFrame()

@pytest.fixture
def join_date(data_frame):
    return data_frame['date_of_join'][0]

@pytest.fixture
def date_str():
    yesterday = datetime.now().date() - timedelta(1)
    str_date = datetime.strftime(yesterday, "%Y-%m-%d")
    return str_date

@pytest.fixture
def s_date():
    str_date = "2022-05-01"
    start_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return start_date


@pytest.fixture
def e_date():
    str_date = "2022-05-03"
    end_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return end_date

@pytest.fixture
def last_run():
    str_date = config["pull_weather_information_from_metaweather_api"]["last_date"]
    last_date = datetime.strptime(str_date, "%Y-%m-%d").date()
    return last_date


@pytest.fixture
def partition(join_date):
    partition_path = join_date.strftime("pt_year=%Y/pt_month=%m/pt_day=%d/")
    return partition_path

@pytest.fixture
def download_path():
    path = os.path.join(
            parent_dir, config["local"]["local_file_path"], "employee_sql"
        )
    return path

@pytest.fixture
def file_name(data_frame,join_date,download_path):
    epoch = int(time())
    file_name = f"employee_{epoch}.json"
    new_df = data_frame[(data_frame.date_of_join == join_date)]
    if not new_df.empty:
        new_df = new_df.astype({"date_of_birth": str, "date_of_join": str})
        pd.DataFrame.to_json(
            new_df,
            download_path + f"/employee_{epoch}.json",
            orient="records",
            lines=True,
        )
    return file_name


@pytest.fixture
def file(file_name):
    file_path = os.path.join(
        parent_dir, config["local"]["local_file_path"], "employee_sql/", file_name
    )
    return file_path


@pytest.fixture
def key(partition, file_name):
    key_name = partition + file_name
    return key_name


@pytest.fixture
def bucket():
    bucket_name = config.get("s3", "bucket_name")
    return bucket_name


@pytest.fixture
def bucket_path():
    bucket_path_name = config.get("s3", "bucket_path")
    return bucket_path_name


class Test_sql_connection:
    """This class will check all success and failure test cases in metaweather api"""

    def test_sql_conncetion_obj(self):
        """This Method will test for the instance belong to the class SqlConnection"""
        self.obj = SqlConnection(logger)
        assert isinstance(self.obj, SqlConnection)
    
    def test_connect_done(self,connection_string):
        """This method will test the whether sql connection is done"""
        self.obj = SqlConnection(logger)
        con_obj = self.obj.connect(connection_string)
        assert con_obj
    
    @pytest.mark.xfail
    def test_connect_not_done(self,wrong_connection_string):
        """This method will test the whether sql connection is done"""
        self.obj = SqlConnection(logger)
        con_obj = self.obj.connect(wrong_connection_string)
        assert con_obj == None
        
    def test_read_query_is_done(self,query):
        """This method will test weather sql quey id read is done"""
        self.obj = SqlConnection(logger)
        response = self.obj.read_query(query)
        print(response)
        assert isinstance(response,pd.DataFrame)

    @pytest.mark.xfail
    def test_read_query_is_not_done(self,):
        """This method will test weather sql quey id read is done"""
        self.obj = SqlConnection(logger)
        response = self.obj.read_query(query)
        assert response is None
    
    def test_where_query_done(self,table,column,s_date,e_date_none,con_param,exclude):
        """This test function will test whether where function successfully get data."""
        self.obj = SqlConnection(logger)
        response = self.obj.where_query(table,column,s_date,e_date_none,con_param,exclude)
        assert isinstance(response,pd.DataFrame)
    
    @pytest.mark.xfail
    def test_where_query_not_done(self,table,column,s_date,e_date,con_param,exclude):
        """This test function will test whether where function successfully get data."""
        self.obj = SqlConnection(logger)
        response = self.obj.where_query(table,column,s_date,e_date,con_param,exclude)
        print(response)
        assert response is None

# class Test_s3:
#     """This class will test all success and failure cases for s3 module"""

#     def test_s3_object(self):
#         """This method test the instance belong to the class of S3Service"""
#         self.s3_obj = S3Service(logger)
#         assert isinstance(self.s3_obj, S3Service)

#     def test_upload_file(self,partition,key,bucket,bucket_path,file):
#         """This method will test file is sucessfully uploaded"""
#         self.my_client = S3Service(logger)
#         self.my_client.upload_file(file, key)
#         self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
#         assert len(self.files) > 0

#     @pytest.mark.xfail
#     def test_upload_file_failed(self,key,bucket,bucket_path,partition,unavailable_file):
#         """This method will test file is not sucessfully uploaded"""
#         self.my_client = S3Service(logger)
#         self.my_client.upload_file(unavailable_file, key)
#         self.files = self.my_client.s3_obj.list_files(bucket, bucket_path + "/" + partition)
#         assert len(self.files) == 0


class Test_pull_sql_employee_data:
    """This class will test all success and failure cases for
    pull_sql_employee_data module"""

    def test_sql_employee_data_object(self, s_date, e_date):  
        """This method test the instance belong to the class of PullWeatherInformationFromMetaWeatherApi"""
        self.obj = PullSqlEmployeeData(s_date, e_date,con_param,exclude)
        assert isinstance(self.obj, PullSqlEmployeeData)

    def test_get_employee_data_where_done(self, s_date, e_date_none, con_param,exclude,data_frame):
        """This method will test whether it get weather information is sucessful"""
        self.obj = PullSqlEmployeeData(s_date, e_date_none,con_param,exclude)
        response = self.obj.get_employee_data_where()
        assert_frame_equal(response,data_frame)
    
    @pytest.mark.xfail        
    def test_get_employee_data_where_not_done(self, s_date, e_date, con_param,exclude):
        """This method will test whether it get weather information is sucessful"""
        self.obj = PullSqlEmployeeData(s_date, e_date,con_param,exclude)
        response = self.obj.get_employee_data_where()
        assert response is None

    def test_create_json_file_done(self,s_date,e_date_none,con_param,exclude,data_frame,join_date):  # --tested
        """This method will test whether it create json file is sucessful"""
        self.obj = PullSqlEmployeeData(s_date, e_date_none,con_param,exclude)
        response = self.obj.create_json_file(data_frame, join_date)
        print(response)
        assert os.path.isfile(response)
        # assert True

    @pytest.mark.xfail
    def test_create_json_file_not_done(self,s_date,e_date_none,con_param,exclude,data_frame,join_date):  # --tested
        """This method will test whether it create json file is sucessful"""
        self.obj = PullSqlEmployeeData(s_date, e_date_none,con_param,exclude)
        response = self.obj.create_json_file(empty_data_frame, join_date)
        assert response is None

    def test_get_partition_done(self,join_date, s_date, e_date,con_param,exclude):  # --tested
        """This method will test whether it get paartition is sucessful"""
        self.obj = PullSqlEmployeeData(s_date, e_date,con_param,exclude)
        try:
            date_obj = datetime.strptime(join_date, "%Y-%m-%dT%H")
            partition_path = date_obj.strftime("pt_year=%Y/pt_month=%m/pt_day=%d/")
        except Exception as err:
            partition_path = None
        response = self.obj.get_partition(join_date)
        assert response == partition_path

    @pytest.mark.xfail
    def test_get_partition_path_not_done(self, join_date, s_date, e_date,con_param,exclude):  # --tested
        """This method will test whether it get paartition is not sucessful"""
        self.obj = PullSqlEmployeeData(s_date, e_date,con_param,exclude)
        try:
            date_obj = datetime.strptime(
                "sfafs", "%Y-%m-%dT%H"
            )  # join_date will be wrong format for failure case
            partition_path = date_obj.strftime("pt_year=%Y/pt_month=%m/pt_day=%d/")
        except Exception as err:
            partition_path = None
        response = self.obj.get_partition("sfsafa")  # join_date will be wrong format for failure case
        assert response == partition_path

    def test_get_date_done(self, date_str):  # tested
        """This method will test whether get date is successful"""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception as err:
            date_obj = None
        response = get_date(date_str)
        assert response == date_obj

    @pytest.mark.xfail
    def test_get_date_not_done(self, date_str):  # --tested
        """This method will test whether get date is not successful"""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception as err:
            date_obj = None
        response = get_date("date_str")
        assert response != date_obj

    def test_set_last_run_done(self):
        """This method wil test the function which update last run in config file sucessfully"""
        set_last_run()
        config = configparser.ConfigParser()
        config.read(parent_dir + "/develop.ini")
        last_run_date = config.get("pull_sql_employee_data", "script_run")
        assert last_run_date == str(datetime.now().date())
        
    def test_get_bool_done(self,bool_str = "True"):
        """This test function will test whether the string converted boolean is done"""
        result =  True if bool_str=='True' else False
        response = get_bool(bool_str)
        assert response == result
    
    @pytest.mark.xfail    
    def test_get_bool_not_done(self,bool_str = "true"):
        """This test function will test whether the string converted boolean is not done"""
        try:
            get_bool(bool_str)
            assert False
        except ValueError:
            assert True

        
    
