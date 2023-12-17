import pandas as pd
import sqlalchemy

def get_mysql_connection():
    user_creds = "root:password"
    db_creds = "localhost:3306"
    db = 'db'
    extras = "charset=utf8mb4"

    engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{user_creds}@{db_creds}/{db}?{extras}")

    connection = engine.connect()

    return connection

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
    "Населённый пункт": "city_code"
}

dataframe = (
    pd.read_csv(
        "web/train.csv", 
        sep=";"
    )
    .rename(columns=mapper)
)

mapper = {
    "NAME": "city", 
    "CODE": "city_code"
}

unique_code = list(dataframe["city_code"].unique())

print("unique_cities")

kladr = (
    pd.read_csv(
        "web/KLADR.csv", 
        sep=";"
    ).rename(columns=mapper)
    [["city", "city_code"]]
)

kladr = kladr[kladr["city_code"].isin(unique_code)]

print(kladr)

with get_mysql_connection() as conn:
    chunk_size = 1_000
    chunks = [dataframe[i:i + chunk_size] for i in range(0, dataframe.shape[0], chunk_size)]

    for chunk in chunks:
        chunk.to_sql(
            "dataframe",
            con=conn,
            if_exists="append",
            index=False
        )

    print("dataframe")

    chunk_size = 1_000
    chunks = [kladr[i:i + chunk_size] for i in range(0, kladr.shape[0], chunk_size)]

    for chunk in chunks:
        chunk.to_sql(
            "kladr",
            con=conn,
            if_exists="append",
            index=False
        )

    print("kladr")