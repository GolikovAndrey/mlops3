import pandas as pd

from scripts.supp_functions import read_mysql_table, write_df_to_mysql, execute_query

def trunc(str):
    return str.split(",")[0]

def prepare_data():
    dataframe = read_mysql_table("SELECT * FROM raw_dataframe")

    for col in dataframe.columns:
        if col not in ["privileges", "sport", "foreign_language"]:
            dataframe = dataframe[dataframe[col].notna()]

    for_norm = read_mysql_table("""
        SELECT 
            MAX(sum_points) AS max_sum_points, 
            MIN(sum_points) AS min_sum_points, 
            MAX(individual_achievements) AS max_individual_achievements, 
            MIN(individual_achievements) AS min_individual_achievements, 
            MAX(age) AS max_age, 
            MIN(age) AS min_age 
        FROM dataframe
    """)
    print("data for normalization received")

    kladr = read_mysql_table("SELECT * FROM kladr")
    print("kladr data received")

    max_sum_points = for_norm["max_sum_points"].astype(int).values[0]
    min_sum_points = for_norm["min_sum_points"].astype(int).values[0]

    max_individual_achievements = for_norm["max_individual_achievements"].astype(int).values[0]
    min_individual_achievements = for_norm["min_individual_achievements"].astype(int).values[0]

    max_age = for_norm["max_age"].astype(int).values[0]
    min_age = for_norm["min_age"].astype(int).values[0]

    dataframe["privileges"][dataframe["privileges"].notnull()] = 1
    dataframe["privileges"][dataframe["privileges"].isnull()] = 0

    dataframe["sport"][dataframe["sport"].notnull()] = 1
    dataframe["sport"][dataframe["sport"].isnull()] = 0

    dataframe["foreign_language"][dataframe["foreign_language"].notnull()] = 1
    dataframe["foreign_language"][dataframe["foreign_language"].isnull()] = 0

    dataframe["city_code"] = dataframe["city_code"].fillna(0).astype("int64")
    kladr["city_code"] = kladr["city_code"].fillna(0).astype("int64")

    dataframe = dataframe.merge(kladr, how="left", on=["city_code"])

    dataframe.loc[dataframe["city"].isna(), "city"] = "Редкие нас. пункты"

    if "city_code" in dataframe.columns:
        print("city_code")
        dataframe.drop(columns=["city_code"], inplace=True)

    dataframe.loc[
        dataframe['education_received'].str.contains('Среднее общее образование'), 
        'education_received'
    ] = 'СОО'

    dataframe.loc[
        dataframe['education_received'].str.contains('Среднее профессиональное образование'), 
        'education_received'
    ] = 'СПО'

    dataframe.loc[
        dataframe['education_received'].str.contains('Высшее образование'), 
        'education_received'
    ] = 'ВО'

    dataframe["army"] = dataframe["army"].apply(trunc)
    dataframe["army"][dataframe["army"].isin(["Да", "да", "ДА", "дА"])] = 1
    dataframe["army"][dataframe["army"].isin(["Нет", "нет", "НЕТ", "нЕт", "неТ"])] = 0

    dataframe["sex"][dataframe["sex"] == "Ж"] = 1
    dataframe["sex"][dataframe["sex"] == "М"] = 0

    dataframe["needs_hostel"][dataframe["needs_hostel"] == "нет"] = 0
    dataframe["needs_hostel"][dataframe["needs_hostel"] == "да"] = 1

    dataframe.loc[dataframe["foreign_language"] != "Нет", "foreign_language"] = 1
    dataframe.loc[dataframe["foreign_language"] == "Нет", "foreign_language"] = 0

    dataframe.loc[dataframe["target_reception"] != "нет", "target_reception"] = 1
    dataframe.loc[dataframe["target_reception"] == "нет", "target_reception"] = 0

    dataframe.loc[dataframe["type_of_reimbursement"] != "бюджет", "type_of_reimbursement"] = 0
    dataframe.loc[dataframe["type_of_reimbursement"] == "бюджет", "type_of_reimbursement"] = 1

    dataframe['sum_points'] = (dataframe['sum_points'] - min_sum_points) / (max_sum_points - min_sum_points)
    dataframe['individual_achievements'] = (dataframe['individual_achievements'] - min_individual_achievements) / (max_individual_achievements - min_individual_achievements)
    dataframe['age'] = (dataframe['age'] - min_age) / (max_age - min_age)

    execute_query("TRUNCATE another_dataframe")
    print("another_dataframe truncated")
    
    write_df_to_mysql(dataframe, "another_dataframe")
    print('data was written')