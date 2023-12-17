import streamlit as st
import sqlalchemy
import pandas as pd
import ds
import model
import json
import ds
import streamlit.components.v1 as components

st.header("Загрузка файла CSV для прогнозирования")

uploaded_file = st.file_uploader("Выберите файл xlsx", type=["xlsx"])

def get_mysql_connection():
    user_creds = "root:password"
    db_creds = "localhost:3306"
    db = 'db'
    extras = "charset=utf8mb4"

    engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}/{db}?{extras}")

    connection = engine.connect()

    return connection

if uploaded_file is not None:
    try:
        st.success("Файл загружен. Делаем прогнозы.")
        components.html(
            "<image src='https://media1.tenor.com/m/FawYo00tBekAAAAC/loading-thinking.gif' width=100 height=100>")
        # st.text("Это заглушка и тут пока ничего не делается")

        mapper = {
            "Пол": "sex",
            "Льготы": "privileges",
            "Нуждается в общежитии": "needs_hostel",
            "Иностранный язык": "foreign_language",
            "Спорт": "sport",
            "Состояние выбран. конкурса": "state_of_selected_competition",
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
            "Возраст": "age",
            "Населённый пункт": "city"
        }

        df = pd.read_excel(uploaded_file, engine="openpyxl").rename(columns=mapper)

        chunk_size = 5_000
        chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

        with get_mysql_connection() as conn:
            for chunk in chunks:
                chunk.to_sql(
                    "raw_dataframe",
                    con=conn,
                    if_exists="append",
                    index=False
                )

        print("dataframe")

    except Exception as e:
        st.error(f"Ошибка при чтении файла: {e}")
