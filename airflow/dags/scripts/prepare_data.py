from supp_functions import read_mysql_table

dataframe = read_mysql_table("SELECT * FROM raw_dataframe")
kladr = read_mysql_table("SELECT * FROM kladr")

dataframe["privileges"][dataframe["privileges"].notnull()] = 1
dataframe["privileges"][dataframe["privileges"].isnull()] = 0

dataframe["sport"][dataframe["sport"].notnull()] = 1
dataframe["sport"][dataframe["sport"].isnull()] = 0

dataframe["foreign_language"][dataframe["foreign_language язык"].isnull()] = "Нет"
dataframe["city_code"] = dataframe["city_code"].astype("int64")

dataframe = dataframe.merge(kladr, how="left", on="city_code")

cities = dataframe[['city']].value_counts()
rare_cities = cities[cities < 100]
dataframe['city'] = dataframe['city'].replace(rare_cities.index.values, 'Редкие нас. пункты')
dataframe['city'][dataframe['city'].isnull()] = 'Редкие нас. пункты'
dataframe['city'].value_counts()