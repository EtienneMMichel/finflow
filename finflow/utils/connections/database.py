import sqlalchemy as sa
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import NoSuchTableError
import os
import pandas as pd
from enum import Enum
from dotenv import load_dotenv
from psycopg2.errors import InvalidColumnReference

load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DATABASE_NAME = os.getenv("DB_NAME")
DATABASE_URL = os.getenv("DATABASE_URL")


class Database():
    def __init__(self):
        user, password, host, port, database_name = USER, PASSWORD, HOST, PORT, DATABASE_NAME
        url = DATABASE_URL  # f'postgresql://{user}:{password}@{host}:{port}/{database_name}'
        url = url.replace("\r", "")
        print("URL: ", url)
        self.engine = sa.create_engine(url)
        self.is_logged = True
        self.metadata = sa.MetaData()
        self.metadata.reflect(bind=self.engine)

    def query(self, q: str):
        '''
        q [str]: SQL query 
        '''
        with self.engine.connect() as connection:
            response = connection.execute(sa.text(q))
        return response

    def set_primary_key(self, table_name, columns):
        key = ", ".join(columns)
        q = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({key});"
        res = self.query(q)
        raise NotImplementedError()

    def get_table(self, table: str, additional_query: str = ""):
        '''
        table [str]: table name
        additional_query [str]: filters
        '''
        q = f'SELECT * FROM "{table}" ' + additional_query
        try:
            r = self.query(q)
            return pd.DataFrame(r)
        except ProgrammingError as e:
            if "UndefinedTable" in str(e):
                return pd.DataFrame()
            else:
                raise e

    def drop_table(self, table: str):
        '''
        table [str]: table to drop
        '''
        q = f"DROP TABLE IF EXISTS {table}"
        return self.query(q)

    def save_dataframe(self, df, table_name, if_exists='append', data_length=None):
        if if_exists == "append":
            with self.engine.begin() as connection:
                df.to_sql(table_name, con=connection, if_exists='append', index=False)
        elif if_exists == "replace":
            try:
                df_in_db = self.get_table(table_name)
            except ProgrammingError as e:
                if "UndefinedTable" in str(e):
                    df_in_db = pd.DataFrame()
                else:
                    raise e
            if not df_in_db.empty:
                df = pd.concat([df_in_db, df], axis=0)
            subset = list(filter(lambda c: c != "data", list(df.columns)))
            df.drop_duplicates(subset=subset, keep='last', inplace=True)
            with self.engine.begin() as connection:
                df.to_sql(table_name, con=connection, if_exists='replace', index=False)

    def set_data(self, records, table_name, data_length=None, conflict_cols=None):
        df = pd.DataFrame(records)
        try:
            df_in_db = self.get_table(table_name)
        except ProgrammingError as e:
            if "UndefinedTable" in str(e):
                df_in_db = pd.DataFrame()
            else:
                raise e
        if not df_in_db.empty:
            df = pd.concat([df_in_db, df], axis=0)
        subset = ["timestamp"]
        df.drop_duplicates(subset=subset, keep='last', inplace=True)
        df.sort_values(by="timestamp", inplace=True)
        if not data_length is None:
            df = df.iloc[-data_length:]
        with self.engine.begin() as connection:
            df.to_sql(table_name, con=connection, if_exists='replace', index=False)

    def upsert_records(self, records: list[dict], table_name: str, conflict_cols: list[str]):
        """
        UPSERT (insert or update) une liste de dictionnaires dans la table donnée.
        - records: liste de dicts [{col: val, ...}, ...]
        - table_name: nom de la table
        - conflict_cols: colonnes de clé (unique/PK)
        """
        def process():
            if not records:
                return

            # Récupérer la table SQLAlchemy
            table = sa.Table(table_name, self.metadata, autoload_with=self.engine)

            # Construire l'INSERT
            insert_stmt = insert(table).values(records)

            # Colonnes à mettre à jour (tout sauf les colonnes de conflit)
            update_dict = {
                c.name: insert_stmt.excluded[c.name]
                for c in table.columns
                if c.name not in conflict_cols
            }

            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_dict,
            )

            # Exécuter
            with self.engine.begin() as conn:
                conn.execute(upsert_stmt)

        try:
            process()
        except NoSuchTableError:
            df = pd.DataFrame(records)
            with self.engine.begin() as connection:
                df.to_sql(table_name, con=connection, if_exists='append', index=False)
            self.create_primary_key(table_name, conflict_cols)

        except (InvalidColumnReference, ProgrammingError) as e:
            # set primary key
            print(f"Clé primaire sur {conflict_cols} manquante. Création...")
            self.create_primary_key(table_name, conflict_cols)
            process()  # réessayer l'UPSERT après avoir créé la PK

    def create_primary_key(self, table_name: str, columns: list[str]):
        """
        Crée une clé primaire composite sur la table donnée si elle n'existe pas déjà.
        """
        key_clause = ", ".join(columns)
        sql = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({key_clause});"
        try:
            with self.engine.begin() as conn:
                conn.execute(sa.text(sql))
            print(f"Primary key ({key_clause}) ajoutée à {table_name}")
        except ProgrammingError as e:
            if "already exists" in str(e):
                print(f"Primary key ({key_clause}) existe déjà sur {table_name}")
            else:
                raise e
    
