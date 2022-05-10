"""This module will used to connect the SQL server with python using pyodbc package"""
from datetime import datetime, timedelta
import pyodbc
import configparser
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
        self.driver = config["sql"]["driver"]
        self.server = config["sql"]["server"]
        self.port = config["sql"]["port"]
        self.user = config["sql"]["user"]
        self.password = config["sql"]["password"]
        self.database = config["sql"]["database"]
        self.logger = logger_obj
        self.conn = self.connect()
        self.logger.info("Object Sucessfully created for class SqlConnection..")

    def connect(self):
        """This is method will make the sql connection with
        and return the connection object"""
        try:
            conn = pyodbc.connect(
                driver=self.driver,
                server=self.server,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            self.logger.info("Connection is established sucessfully..")
        except Exception as err:
            self.logger.info("Connection not is established ..")
            print(err)
            conn = None
        return conn

    def execute_query(self, query):
        """This is method will make the sql connection with
        and return the connection object"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.logger.info("Cursor is established sucessfully..")
        except Exception as err:
            self.logger.info("Connection not is established ..")
            print(err)
            cursor = None
        return cursor
    
    def where_query(self,start=None,end=None,con_param='=',exclude=False):
        """This method will retrieve the data based on the where query conditions"""
        if not end :
            query = f"SELECT * FROM Employee  WHERE date_of_join {con_param}"\
                f"'{start}'"
        else :
            end = end if exclude else end - timedelta(1)
            query = "SELECT * FROM Employee  WHERE date_of_join "\
                f"{con_param}'{start}' AND '{end}'"
            print(query)
        response = self.execute_query(query)
        return response
        


# print(pyodbc.drivers())