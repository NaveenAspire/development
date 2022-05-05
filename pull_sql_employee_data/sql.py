"""This module will used to connect the SQL server with python using pyodbc package"""
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
# cursor.execute('select * from information_schema.tables')
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

    def retrieve(self, query):
        """This is method will make the sql connection with
        and return the connection object"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            self.logger.info("Cursor is established sucessfully..")
        except Exception as err:
            self.logger.info("Connection not is established ..")
            print(err)
            response = None
        return response
