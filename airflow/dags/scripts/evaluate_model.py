import sqlalchemy
import pandas as pd
from supp_functions import read_mysql_table

from airflow.models import Variable



dataframe = read_mysql_table("SELECT * FROM dataframe")

