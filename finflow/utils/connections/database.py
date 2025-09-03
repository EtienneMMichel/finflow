import sqlalchemy as sa
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import ProgrammingError
import os
import pandas as pd
from enum import Enum
from dotenv import load_dotenv
load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DATABASE_NAME = os.getenv("DB_NAME")


class Database():
    def __init__(self):
        user, password, host, port, database_name = USER, PASSWORD, HOST, PORT, DATABASE_NAME
        url = f'postgresql://{user}:{password}@{host}:{port}/{database_name}'
        url = url.replace("\r", "")
        self.engine = sa.create_engine(url)
        self.is_logged = True

    def query(self, q: str):
        '''
        q  [str]: SQL query 
        '''
        with self.engine.connect() as connection:
            response = connection.execute(sa.text(q))
        return response

    def get_table(self, table: str, additional_query: str = ""):
        '''
        table [str]: table name
        additional_query [str]: filters
        '''
        q = f"SELECT * FROM \"{table}\" " + additional_query
        try:
            r = self.query(q)
            return pd.DataFrame(r)
        except ProgrammingError as e:
            if "UndefinedTable" in str(e):
                return pd.DataFrame()
            else: raise e

    def drop_table(self, table: str):
        '''
        table [str]: table to drop
        '''
        q = f"DROP TABLE IF EXISTS {table}"
        return self.query(q)
    
    def save_dataframe(self, df, table_name, if_exists='append'):
        if if_exists == "append":
            with self.engine.begin() as connection:
                df.to_sql(table_name, con=connection, if_exists='append', index=False)
        elif if_exists == "replace":
            try:
                df_in_db = self.get_table(table_name)
            except ProgrammingError as e:
                if "UndefinedTable" in str(e):
                    df_in_db = pd.DataFrame()
                else: raise e
            if not df_in_db.empty:
                df = pd.concat([df_in_db, df], axis=0)
            subset = list(filter(lambda c:c != "data", list(df.columns)))
            df.drop_duplicates(subset = subset,keep='last', inplace=True)
            with self.engine.begin() as connection:
                df.to_sql(table_name, con=connection, if_exists='replace', index=False)


    def set_data(self, records, table_name, data_length=None):
        df = pd.DataFrame(records)
        try:
            df_in_db = self.get_table(table_name)
        except ProgrammingError as e:
            if "UndefinedTable" in str(e):
                df_in_db = pd.DataFrame()
            else: raise e
        if not df_in_db.empty:
            df = pd.concat([df_in_db, df], axis=0)
        subset = ["timestamp"] # list(filter(lambda c:c != "data", list(df.columns)))
        df.drop_duplicates(subset = subset,keep='last', inplace=True)
        df.sort_values(by="timestamp", inplace=True)
        # df.reset_index(drop=True, inplace=True)
        if not data_length is None:
            df = df.iloc[-data_length:]
        with self.engine.begin() as connection:
            df.to_sql(table_name, con=connection, if_exists='replace', index=False)