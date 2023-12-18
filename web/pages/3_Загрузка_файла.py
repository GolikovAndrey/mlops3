import os
import time
import mlflow
import streamlit as st
from datetime import timedelta
import sqlalchemy
import pandas as pd
import streamlit.components.v1 as components
import requests
from sqlalchemy import text

st.header("Загрузка файла CSV для прогнозирования")

uploaded_file = st.file_uploader("Выберите файл xlsx", type=["xlsx"])

def get_mysql_connection():
    user_creds = "root:password"
    db_creds = "127.0.0.1:3306"
    db = 'db'
    extras = "charset=utf8mb4"

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
    chunk_size = 1_000
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

if uploaded_file is not None:
    try:
        st.success("Файл загружен. Делаем прогнозы.")
        components.html("<image src='https://media1.tenor.com/m/FawYo00tBekAAAAC/loading-thinking.gif' width=100 height=100>")
        # st.text("Это заглушка и тут пока ничего не делается")

        mapper = {
            "Пол": "sex",
            "Льготы": "privileges",
            "Нуждается в общежитии": "needs_hostel",
            "Иностранный язык": "foreign_language",
            "Спорт": "sport",
            "Служба в армии": "army",
            "Полученное образование": "education_received",
            "Форма получения док. об образ.": "document_on_education",
            "Вид возмещения затрат": "type_of_reimbursement",
            "Форма обучения": "edu_form",
            "Вид приема": "reception_type",
            "Формирующее подр.": "forming_unit",
            "Набор ОП": "educational_programs",
            "Целевой прием": "target_reception",
            "Итоговое согласие": "final_consent",
            "Сумма баллов": "sum_points",
            "Сумма баллов за индивидуальные достижения": "individual_achievements",
            "Код насел. пункта": "city_code",
            "Возраст": "age"
        }

        dataframe = (
            pd.read_excel(
                uploaded_file, 
                engine="openpyxl"
            )
        )

        dataframe['Дата подачи'] = pd.to_datetime(dataframe['Дата подачи'], format='mixed')
        dataframe['Дата рождения'] = pd.to_datetime(dataframe['Дата рождения'], format='mixed')
        dataframe['Возраст'] = ((dataframe['Дата подачи'] - dataframe['Дата рождения']).dt.days/365).fillna(0).astype(int)

        dataframe.loc[dataframe['Возраст'] < 17, 'Возраст'] = 17
        dataframe.loc[dataframe['Возраст'] > 25, 'Возраст'] = 25

        dataframe = (
            dataframe[[
                'Пол', 'Возраст', 'Льготы', 'Нуждается в общежитии', 
                'Иностранный язык', 'Спорт', 'Код насел. пункта', 'Служба в армии', 'Полученное образование', 
                'Форма получения док. об образ.', 
                'Вид возмещения затрат', 'Форма обучения', 'Вид приема',  'Формирующее подр.', 
                'Набор ОП', 'Целевой прием', 'Итоговое согласие', 'Сумма баллов', 
                'Сумма баллов за индивидуальные достижения'
            ]]
            .rename(columns=mapper)
        )

        st.success("Данные предобработаны, загружаем в базу данных.")

        execute_query("TRUNCATE TABLE raw_dataframe")
        write_df_to_mysql(dataframe, "raw_dataframe")

        try:
            response = requests.post(
                "http://127.0.0.1:8080/api/v1/dags/prepare_data_dag/dagRuns", 
                headers={'Content-Type': 'application/json'},
                auth=("airflow", "airflow"),
                json={}
            ).json()

            st.success("Отправлена задача на предобработку данных.")
            print(response["dag_run_id"])
        except Exception as e:
            st.error(f"Ошибка при отправке данных на предобработку: {e}")

        time.sleep(10)
        
        st.success("Данные успешно предобработаны. Сделаем предсказание.")

        os.environ["MLFLOW_TRACKING_URI"] = "http://172.30.0.15:5000"
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://172.30.0.15:9000"
        os.environ["AWS_ACCESS_KEY_ID"] = 'minio'
        os.environ["AWS_SECRET_ACCESS_KEY"] = 'minio123'
        os.environ["AWS_DEFAULT_REGION"] = 'eu-central-1'

        model_name = "CatBoostClassifier"
        model_stage = "None"

        mlflow.set_tracking_uri("http://172.30.0.15:5000")

        client = mlflow.MlflowClient(tracking_uri="http://172.30.0.15:5000")
        model_version = client.get_latest_versions(model_name, stages=[model_stage])[0].version
        print(f'Для модели {model_name} со стадией {model_stage} самой поздней версией является {model_version}')
        model = mlflow.catboost.load_model(model_uri=f'models:/{model_name}/{model_stage}')

        df = read_mysql_table("SELECT * FROM another_dataframe")

        st.dataframe(df)

        predict = model.predict(df)

        st.dataframe(predict)

    except Exception as e:
        st.error(f"Ошибка при чтении файла: {e}")
