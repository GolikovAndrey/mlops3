import sqlalchemy
import pandas as pd
from airflow.models import Variable

CONN_PARAMS = {
    "MYSQL_USER": Variable.get("MYSQL_USER"),
    "MYSQL_PASS": Variable.get("MYSQL_PASS"),
    "MYSQL_HOST": Variable.get("MYSQL_HOST"),
    "MYSQL_PORT": Variable.get("MYSQL_PORT"),
    "MYSQL_DB": Variable.get("MYSQL_DB")
}

user_creds = f'{CONN_PARAMS["MYSQL_USER"]}:{CONN_PARAMS["MYSQL_PASS"]}'
db_creds = f'{CONN_PARAMS["MYSQL_HOST"]}:{CONN_PARAMS["MYSQL_PORT"]}'
db = CONN_PARAMS["MYSQL_DB"]
extras = "charset=utf8mb4"

def get_mysql_connection():

    engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}/{db}?{extras}")

    connection = engine.connect()

    return connection

def read_mysql_table(query) -> pd.DataFrame:
        """
        Метод для чтения таблицы из MySQL.
        Параметр where_statement подаётся в виде "where ..."
        """
        with get_mysql_connection() as mysql_conn:
            dataframe = pd.read_sql(
                sql=query,
                con=mysql_conn
            )

        return dataframe

def write_df_to_mysql(dataframe: pd.DataFrame):
    chunk_size = 5_000
    chunks = [dataframe[i:i + chunk_size] for i in range(0, dataframe.shape[0], chunk_size)]

    with get_mysql_connection() as conn:
        for chunk in chunks:
            chunk.to_sql(
                "raw_dataframe",
                con=conn,
                if_exists="append",
                index=False
            )