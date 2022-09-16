import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from os.path import exists
import logging

log = logging.getLogger("Logging...")
log.info("data cleansing starts")


excel_file_path = r"C:\Users\Hamit_DNCAcc\PycharmProjects\AustraliaRainPrediction\venv\Files\weatherAUS.csv"
file_exists = exists(excel_file_path)

if file_exists:
    log.info(f"File is exist in this folder {excel_file_path}")
    df = pd.read_csv(excel_file_path)
else:
    log.info(f"File is not exist in this folder {excel_file_path}")

#print(df.head(5))

def na_per_each_column_then_delete_50000(df):
    list_of_delete = []
    for column in df.columns:
        print(f"Number of NA for {column} - {df[column].isna().sum()}")
        if df[column].isna().sum() > 50000:
            list_of_delete.append(column)
    new_df = df.drop(columns=list_of_delete,axis=1)
    new_df = new_df.dropna()
    return new_df

df = na_per_each_column_then_delete_50000(df)
df = na_per_each_column_then_delete_50000(df)