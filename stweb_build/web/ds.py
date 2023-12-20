import sqlalchemy
from sqlalchemy import text
import pandas as pd

CONN_PARAMS = {
    "MYSQL_USER": "root", 
    "MYSQL_PASS": "password", 
    "MYSQL_HOST": "127.0.0.1",
    "MYSQL_PORT": 3306,
    "MYSQL_DB": "db"
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

def normalize_sum_ball(ball, max, min):
    ball = (ball - min) / (max - min)
    return ball


def mormalize_achieve_ball(ball, max, min):
    ball = (ball - min) / (max - min)
    return ball


def normalize_Age(age, max, min):
    age = (age - min) / (max - min)


def get_cites(df):
    return df['city'].unique()


def get_op(df):
    return df['educational_programs'].unique()


def get_edu(df):
    return df['education_received'].unique()


def get_faculty(df):
    return df['forming_unit'].unique()


def get_payment(df):
    return df['type_of_reimbursement'].unique()


def get_form(df):
    return df['edu_form'].unique()


def get_enroll_type(df):
    return df['reception_type'].unique()