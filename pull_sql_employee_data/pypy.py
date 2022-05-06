import pypyodbc as pdb
import pandas as pd


connection = pdb.odbc("""
	driver = '{SQL Server Native Client 11.0}',
                      server = '127.0.0.1',
                       port='1433',
                       user='SA',
                       password='Chokiee@Naveen10',
                       database='Payroll'
""")

query = """SELECT * FROM Employee"""
table = pd.read_sql(query, connection)

