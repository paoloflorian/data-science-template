import mysql.connector
import pandas as pd
from mysql.connector import Error

from .setup import Setup
from .timer import Timer

class MAD:
    def __init__(self, credentials: "Setup.Credentials"):
        self.host = credentials.host
        self.user = credentials.username
        self.password = credentials.password
        self.database = credentials.database
        self.charset = credentials.charset
        self.mycon = None

    def open_connection(self):
        try:
            self.mycon = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset
            )
            if self.mycon.is_connected():
                db_info = self.mycon.get_server_info()
                print('Connected to MySQL Server version', db_info)
                cursor = self.mycon.cursor()
                cursor.execute('SELECT DATABASE();')
                record = cursor.fetchone()
                print("You're connected to database:", record)
        except Error as e:
            print('Error while connecting to MySQL:', e)

    def close_connection(self):
        if self.mycon and self.mycon.is_connected():
            self.mycon.close()
            print('MySQL connection is closed')
        else:
            print('MySQL connection is already closed')
    
    def execute_query(self, query):
        if self.mycon and self.mycon.is_connected():
            try:
                Timer.Display('Retrivieng data from',f' {self.database} [..]',cr=0)
                df = pd.read_sql(query, self.mycon)
                Timer.Display('Retrivied data from',f' {self.database}',cr=1)
                return df
            except Error as e:
                print('Error executing query:', e)
                return None
        else:
            print('Connection is not open')
            return None
    def SaveFile(self,df, fileName):
        Timer.Display('Saving file',f' {fileName} [..]',cr=0)
        df.to_excel(fileName, index=False)
        Timer.Display('Saved file',f' {fileName}',cr=1)

# Esempio di utilizzo:
# db = MAD()
# db.open_connection()
# df = db.execute_query("SELECT * FROM some_table")
# print(df)
# db.close_connection()
