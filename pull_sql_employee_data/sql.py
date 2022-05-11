"""This module will used to connect the SQL server with python using pyodbc package"""
from datetime import datetime, timedelta
import sys
import pyodbc
import configparser
from sqlalchemy.engine import URL,create_engine
import pandas as pd
import os

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")
# conn = pyodbc.connect(driver = '{ODBC Driver 17 for SQL Server}',
#                       server = '127.0.0.1',
#                       port='1433',
#                       user='SA',
#                       password='Chokiee@Naveen10',
#                       database='Payroll'
#                     )

# print(conn)
# cursor = conn.cursor()
# cursor.execute("select * from Employee  where date_of_join >= '2022-05-01' AND date_of_join < '2022-05-02'")
# res = cursor.fetchall()

# print(res)


class SqlConnection:
    """This is the class which contains methods for connecting SQL with python"""

    def __init__(self, logger_obj) -> None:
        """This is the init method for class of SqlConnection"""
        self.connection_string = config["sql"]["connection_string"]
        self.logger = logger_obj
        self.conn = self.connect()
        self.logger.info("Object Sucessfully created for class SqlConnection..")

    def connect(self):
        """This is method will make the sql connection with
        and return the connection object"""
        try:
            connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": self.connection_string})
            conn = create_engine(connection_url)
            self.logger.info("Connection is established sucessfully..")
        except Exception as err:
            self.logger.info("Connection not is established ..")
            print(err)
            conn = None
        return conn

    def read_query(self, query):
        """This is method will make the sql connection with
        and return the connection object"""
        try:
            df_list =[]
            for chunk in pd.read_sql(query,self.conn,chunksize=1000):
                df_list.append(chunk)
            data_frame = pd.concat(df_list)
            self.logger.info("Query read sucessfully..")
        except Exception as err:
            self.logger.info("Connection not is established ..")
            print(err)
            data_frame = None
        return data_frame
    
    def where_query(self,table,column,start,end=None,con_param='=',exclude=False):
        """This method will retrieve the data based on the where query conditions"""
        try :
            con_param = con_param.upper()
            if not end and con_param != 'BETWEEN' :
                query = f"SELECT * FROM {table}  WHERE {column} {con_param}"\
                    f"'{start}'"
            elif end and con_param == 'BETWEEN' :
                end = end - timedelta(1) if exclude else end
                print(type(exclude))
                query = f"SELECT * FROM {table}  WHERE {column} "\
                    f"{con_param}'{start}' AND '{end}'"
            else :
                print("You were given wrong operator for given date")
                sys.exit()
            data_frame = self.read_query(query)
            print(data_frame)
        except Exception as err :
            print(err)
            data_frame = None
        return data_frame
        


# print(pyodbc.drivers())