import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.datasets import make_classification
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import LinearSVC, SVC
from xgboost import XGBClassifier
from collections import Counter
from mlxtend.plotting import plot_decision_regions
from fastparquet import write
from os.path import exists
import logging
from datacleansing_1 import control_of_file
import datetime

parquet_file_path = r"C:\Users\Hamit_DNCAcc\PycharmProjects\weather_australia_prediction\ParquetFiles\df_today.parq"

def drop_location(city_name, df):
    df = df.drop(city_name, axis=1)
    return df


def make_object_to_date(df):
    df["Date"] = pd.to_datetime(df["Date"])
    df["Year"], df["Month"], df["Day"] = pd.DatetimeIndex(df['Date']).year, pd.DatetimeIndex(
        df['Date']).month, pd.DatetimeIndex(df['Date']).day
    df = df.drop("Date", axis=1)
    for column in df.columns:
        df[column] = df[column].astype(float)
    return df


df, df_temp = control_of_file(parquet_file_path)
df_albury = df.loc[df["Location"] == "Albury"]
print("Number of Location : ", df_albury["Location"].nunique())
print(df_albury["RainToday"].value_counts())

df_albury = drop_location("Location", df_albury)
df_albury = make_object_to_date(df_albury)

print(df_albury.info())
