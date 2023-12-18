import os
import numpy as np
import pandas as pd
import mlflow
from catboost import CatBoostClassifier


os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://localhost:9000"

model_name = "CatBoostClassifier"
model_stage = 'None'

mlflow.set_tracking_uri("http://localhost:5000")
# mlflow.set_registry_uri("http://localhost:9000")

client = mlflow.MlflowClient(tracking_uri="http://172.30.0.15:5000")
model_version = client.get_latest_versions(model_name, stages=[model_stage])[0].version
print(f'Для модели {model_name} со стадией {model_stage} самой поздней версией является {model_version}')
model = mlflow.catboost.load_model(model_uri=f'models:/{model_name}/{model_stage}')

df = pd.DataFrame({"education_received": ['a', 'eq', 'w','f', 'q','s', 'g']})

# predict = model.predict(df)

print(model)

# CBR = CatBoostClassifier()

# d = pd.DataFrame({"a": [1,2,3,4,5]})
# f = np.ravel(pd.DataFrame({"b": [2,3,1,2,5]})["b"])

# CBR.fit(X=d, y=f)


# mlflow.set_tracking_uri("http://localhost:5000")
# # mlflow.set_registry_uri("http://localhost:9000")
# mlflow.set_experiment("CatBoostClassifier")

# with mlflow.start_run() as run:
#     mlflow.catboost.log_model(
#         CBR,
#         "CatBoostClassifier"
#     )

#     mlflow.register_model(
#         model_uri=f"runs:/{run.info.run_id}/CatBoostClassifier",
#         name="CatBoostClassifier"
#     )


# import pandas as pd
# from sqlalchemy import create_engine, text
# def get_mysql_connection():
#     user_creds = "root:password"
#     db_creds = "127.0.0.1:3306"
#     db = 'db'
#     extras = "charset=utf8mb4"

#     engine = create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}/{db}?{extras}")

#     connection = engine.connect()

#     return connection

# def read_mysql_table(query) -> pd.DataFrame:
#         """
#         Метод для чтения таблицы из MySQL.
#         Параметр where_statement подаётся в виде "where ..."
#         """
#         with get_mysql_connection() as mysql_conn:
#             dataframe = pd.read_sql(
#                 sql=query,
#                 con=mysql_conn
#             )

#         return dataframe

# def write_df_to_mysql(dataframe: pd.DataFrame, table_name: str):
#     chunk_size = 5_000
#     chunks = [dataframe[i:i + chunk_size] for i in range(0, dataframe.shape[0], chunk_size)]

#     with get_mysql_connection() as conn:

#         for chunk in chunks:
#             chunk.to_sql(
#                 table_name,
#                 con=conn,
#                 if_exists="append",
#                 index=False
#             )

# def execute_query(query: str):
#     with get_mysql_connection() as conn:
#         conn.execute(text(query))


# mapper = {
#     "Пол": "sex",
#     "Льготы": "privileges",
#     "Нуждается в общежитии": "needs_hostel",
#     "Иностранный язык": "foreign_language",
#     "Спорт": "sport",
#     "Состояние выбран. конкурса": "state_of_selected_competition",
#     "Служба в армии": "army",
#     "Полученное образование": "education_received",
#     "Форма получения док. об образ.": "document_on_education",
#     "Вид возмещения затрат": "type_of_reimbursement",
#     "Форма обучения": "edu_form",
#     "Вид приема": "reception_type",
#     "Формирующее подр.": "forming_unit",
#     "Набор ОП": "educational_programs",
#     "Целевой прием": "target_reception",
#     "Итоговое согласие": "final_consent",
#     "Сумма баллов": "sum_points",
#     "Сумма баллов за индивидуальные достижения": "individual_achievements",
#     "Возраст": "age",
#     "Населённый пункт": "city"
# }

# dataframe = (
#     pd.read_csv(
#         "web/train.csv", 
#         sep=";"
#     )
#     .rename(columns=mapper)
# )

# dataframe['state_of_selected_competition'][dataframe['state_of_selected_competition'].isin(["Сданы ВИ", "Забрал документы", "Выбыл из конкурса", "Отказ от зачисления", "Исключен (зачислен на другой конкурс)", "Активный"])] = 0
# dataframe['state_of_selected_competition'][dataframe['state_of_selected_competition'].isin(["Зачислен"])] = 1

# mapper = {
#     "NAME": "city", 
#     "CODE": "city_code"
# }

# kladr = (
#     pd.read_csv(
#         "web/KLADR.csv", 
#         sep=";"
#     ).rename(columns=mapper)
#     [["city", "city_code"]]
# )

# usc = (
#     pd.read_csv(
#         "web/uniq_city_codes.csv",
#         sep=','
#     )
# )

# kladr = kladr[kladr["city_code"].isin(usc["Код насел. пункта"].to_list())]

# # execute_query("TRUNCATE kladr")

# with get_mysql_connection() as conn:

#     chunk_size = 1_000
#     chunks = [dataframe[i:i + chunk_size] for i in range(0, dataframe.shape[0], chunk_size)]

#     for chunk in chunks:
#         chunk.to_sql(
#             "dataframe",
#             con=conn,
#             if_exists="append",
#             index=False
#         )

#     print("dataframe")

#     chunk_size = 1_000
#     chunks = [kladr[i:i + chunk_size] for i in range(0, kladr.shape[0], chunk_size)]

#     for chunk in chunks:
#         chunk.to_sql(
#             "kladr",
#             con=conn,
#             if_exists="append",
#             index=False
#         )

#     print("kladr")