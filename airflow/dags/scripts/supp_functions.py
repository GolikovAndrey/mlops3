import os
import mlflow
import sqlalchemy
import pandas as pd
from sqlalchemy import text
from airflow.models import Variable

CONN_PARAMS = {
    "MYSQL_USER": Variable.get("MYSQL_USER"),
    "MYSQL_PASS": Variable.get("MYSQL_PASS"),
    "MYSQL_HOST": Variable.get("MYSQL_HOST"),
    "MYSQL_PORT": Variable.get("MYSQL_PORT"),
    "MYSQL_DB": Variable.get("MYSQL_DB"),

    "MLFLOW_TRACKING_URI": Variable.get("MLFLOW_TRACKING_URI")
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

def write_df_to_mysql(dataframe: pd.DataFrame, table_name: str):
    chunk_size = 5_000
    chunks = [dataframe[i:i + chunk_size] for i in range(0, dataframe.shape[0], chunk_size)]

    with get_mysql_connection() as conn:

        for chunk in chunks:
            chunk.to_sql(
                table_name,
                con=conn,
                if_exists="append",
                index=False
            )

def execute_query(query: str):
    with get_mysql_connection() as conn:
        conn.execute(text(query))

def mlflow_get_model(model_name, model_stage):
    mlflow.set_tracking_uri(CONN_PARAMS['MLFLOW_TRACKING_URI'])
    if model_stage == None:
        model_stage = "None"
    client = mlflow.MlflowClient(tracking_uri=CONN_PARAMS['MLFLOW_TRACKING_URI'])
    model_version = client.get_latest_versions(model_name, stages=[model_stage])[0].version
    print(f'Для модели {model_name} со стадией {model_stage} самой поздней версией является {model_version}')
    model = mlflow.pyfunc.load_model(model_uri=f'models:/{model_name}/{model_stage}')
    print(model)

    return model
