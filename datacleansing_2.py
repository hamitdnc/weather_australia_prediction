import pandas as pd
from fastparquet import write
from os.path import exists
import logging
from datacleansing_1 import control_of_file

parquet_file_path = r"C:\Users\Hamit_DNCAcc\PycharmProjects\weather_australia_prediction\ParquetFiles\df_today.parq"

df, df_temp = control_of_file(parquet_file_path)

print(df.head())

df_albury = df.loc[df["Location"] == "Albury"]

print(df_albury["Location"].value_counts())

