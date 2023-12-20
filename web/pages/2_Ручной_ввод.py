import os
import ds
import mlflow
import pandas as pd
import streamlit as st

st.header("Прогнозирование поступления ПГГПУ")

os.environ["MLFLOW_TRACKING_URI"] = "http://172.30.0.15:5000"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://172.30.0.15:9000"
os.environ["AWS_ACCESS_KEY_ID"] = 'minio'
os.environ["AWS_SECRET_ACCESS_KEY"] = 'minio123'
os.environ["AWS_DEFAULT_REGION"] = 'eu-central-1'

df = ds.read_mysql_table("SELECT * FROM dataframe")

for_norm = ds.read_mysql_table("""
    SELECT 
        MAX(sum_points) AS max_sum_points, 
        MIN(sum_points) AS min_sum_points, 
        MAX(individual_achievements) AS max_individual_achievements, 
        MIN(individual_achievements) AS min_individual_achievements, 
        MAX(age) AS max_age, 
        MIN(age) AS min_age 
    FROM dataframe
""")

max_sum_points = for_norm["max_sum_points"].astype(int).values[0]
min_sum_points = for_norm["min_sum_points"].astype(int).values[0]

max_individual_achievements = for_norm["max_individual_achievements"].astype(int).values[0]
min_individual_achievements = for_norm["min_individual_achievements"].astype(int).values[0]

max_age = for_norm["max_age"].astype(int).values[0]
min_age = for_norm["min_age"].astype(int).values[0]

kladr = ds.read_mysql_table("SELECT * FROM kladr")

with st.form("my_form"):
    with st.container(border=True):
        sex = st.radio(
            "Пол",
            ["М", "Ж"],
            index=None,
        )

    with st.container(border=True):
        benefit = st.checkbox('Льгота')

    with st.container(border=True):
        hostel = st.checkbox('Нуждается в общежитии')

    with st.container(border=True):
        language = st.selectbox(
            'Иностранный язык',
            ("Изучался", "Не изучался"), 
            placeholder="Иностранный язык"
        )

    with st.container(border=True):
        sport = st.checkbox('Спорт')

    with st.container(border=True):
        army = st.checkbox('Служба в армии')

    with st.container(border=True):
        education = st.selectbox(
            'Полученное образование',
            (ds.get_edu(df))
        )

    with st.container(border=True):
        document_educatuon = st.radio(
            "Форма получения док. об образ.",
            ["Оригинал", "Копия"],
            index=None,
        )

    with st.container(border=True):
        pay_type = st.radio(
            "Вид возмещения затрат",
            ["Бюджет", "Договор"],
            index=None,
        )

    with st.container(border=True):
        education_form = st.selectbox(
            'Форма обучения',
            (ds.get_form(df))
        )

    with st.container(border=True):
        reception_type = st.selectbox(
            'Вид приема',
            (ds.get_enroll_type(df))
        )

    with st.container(border=True):
        department = st.selectbox(
            'Формирующее подр.',
            (ds.get_faculty(df))
        )

    with st.container(border=True):
        education_program = st.selectbox(
            'Набор ОП',
            (ds.get_op(df))
        )

    with st.container(border=True):
        targeted_reception = st.checkbox('Целевой прием')

    with st.container(border=True):
        total_points = st.number_input('Сумма баллов', min_value=0, max_value=400)

    with st.container(border=True):
        total_achievements_points = (
            st.number_input(
                'Сумма баллов за индивидуальные достижения', 
                min_value=0, 
                max_value=10
            )
        )

    with st.container(border=True):
        age = st.number_input('Возраст', min_value=17, max_value=25)

    with st.container(border=True):
        settlement = st.selectbox(
            'Населенный пункт',
            (ds.get_cites(df))
        )

    submitted = st.form_submit_button("Сформировать прогноз")

    if submitted:
        if not sex:
            st.error("Отметьте пол")

        elif not language:
            st.error("Выберите языки")
        
        elif not education:
            st.error("Выберите полученное образование")
        
        elif not document_educatuon:
            st.error("Выберите форму получения документа об образовании")
        
        elif not pay_type:
            st.error("Выберите вид возмещения затрат")
        
        elif not education_form:
            st.error("Выберите форму обучения")
        
        elif not reception_type:
            st.error("Выберите вид приема")
        
        elif not department:
            st.error("Выберите формирующее подразделение")
        
        elif not education_program:
            st.error("Выберите набор образовательных программ")
        
        elif not total_points:
            st.error("Введите сумму баллов")
        
        elif not total_achievements_points:
            st.error("Введите сумму баллов за индивидуальные достижения")
        
        elif not age:
            st.error("Введите возраст")
        
        elif not settlement:
            st.error("Выберите населенный пункт")
        
        else:
            params_dict = {
                'sex': [0 if sex == 'М' else 1],
                'privileges': [0 if benefit == False else 1],
                'needs_hostel': [0 if hostel == False else 1],
                'foreign_language': [1 if language == 'Изучался' else 0],
                'sport': [0 if sport == False else 1],
                'army': [0 if army == False else 1],
                'education_received': [education],
                'document_on_education': [document_educatuon],
                'type_of_reimbursement': [0 if pay_type == 'Договор' else 1],
                'edu_form': [education_form],
                'reception_type': [reception_type],
                'forming_unit': [department],
                'educational_programs': [education_program],
                'target_reception': [1 if targeted_reception == True else 0],
                'sum_points': [
                    ds.normalize_sum_ball(
                        int(total_points), 
                        max_sum_points, 
                        min_sum_points
                    )
                ],
                'individual_achievements': [
                    ds.mormalize_achieve_ball(
                        int(total_achievements_points), 
                        max_individual_achievements, 
                        min_individual_achievements
                    )
                ],
                'age': [ds.normalize_Age(int(age), max_age, min_age)],
                'city': settlement
            }
            print(params_dict)

            data = pd.DataFrame(params_dict)

            model_name = "CatBoostClassifier"
            model_stage = "None"
            mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
            client = mlflow.MlflowClient(tracking_uri=os.getenv("MLFLOW_TRACKING_URI"))
            model_version = client.get_latest_versions(model_name, stages=[model_stage])[0].version
            print(f'Для модели {model_name} со стадией {model_stage} самой поздней версией является {model_version}')
            model = mlflow.catboost.load_model(model_uri=f'models:/{model_name}/{model_stage}')

            result = model.predict(data)
            
            if result == 0:
                st.warning("Результат прогноза: НЕ ПОСТУПИТ")
            else:
                st.success("Результат прогноза: ПОСТУПИТ")
