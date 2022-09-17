import pandas as pd
from fastparquet import write
from os.path import exists
import logging

log = logging.getLogger("Logging...")
log.info("data cleansing starts")
excel_file_path = r"C:\Users\Hamit_DNCAcc\PycharmProjects\weather_australia_prediction\Files\weatherAUS.csv"


def control_of_file(excel_file_path):
    file_exists = exists(excel_file_path)
    if file_exists:
        log.info(f"File is exist in this folder {excel_file_path}")
        try:
            df = pd.read_csv(excel_file_path)
        except:
            print("CSV method did not work ")
        try:
            df = pd.read_parquet(excel_file_path)
        except:
            print("Parquet method did not work ")
        temp_df = df
        return df, temp_df
    else:
        log.info(f"File is not exist in this folder {excel_file_path}")


def na_per_each_column_then_delete_50000(df):
    list_of_delete = []
    for column in df.columns:
        print(f"Number of NA for {column} - {df[column].isna().sum()}")
        if df[column].isna().sum() > 50000:
            list_of_delete.append(column)
    new_df = df.drop(columns=list_of_delete, axis=1)
    new_df = new_df.dropna()
    return new_df


def one_hot_some_columns(df, columns_for_one_hot):
    df = pd.get_dummies(data=df, columns=columns_for_one_hot, drop_first=True)
    return df


def split_today_tomorrow(df):
    df_column_rain_today = df["RainToday"].apply(lambda x: 1 if x == "Yes" else 0)
    df_column_rain_tomorrow = df["RainTomorrow"].apply(lambda x: 1 if x == "Yes" else 0)
    return df_column_rain_today, df_column_rain_tomorrow


def drop_columns(df, columns_):
    df = df.drop(columns=columns_, axis=1)
    return df


def merge_today_tomorrow_series(df, df_column_rain_today, df_column_rain_tomorrow):
    df_today = df.merge(df_column_rain_today.to_frame(), left_index=True, right_index=True)
    df_tomorrow = df.merge(df_column_rain_tomorrow.to_frame(), left_index=True, right_index=True)
    return df_today, df_tomorrow


def dataframe_to_parquet(file_path_name, df):
    write(file_path_name, df)


if __name__ == "__datacleansing_1__":
    df, temp_df = control_of_file(excel_file_path)
    df = na_per_each_column_then_delete_50000(df)
    df = na_per_each_column_then_delete_50000(df)

    columns_for_one_hot = ["WindGustDir", "WindDir9am", "WindDir3pm"]
    df = one_hot_some_columns(df, columns_for_one_hot)

    df_column_rain_today, df_column_rain_tomorrow = split_today_tomorrow(df)

    columns_to_drop = ["RainToday", "RainTomorrow"]
    df = drop_columns(df, columns_to_drop)

    df_today, df_tomorrow = merge_today_tomorrow_series(df, df_column_rain_today, df_column_rain_tomorrow)

    # df.to_excel("output.xlsx")
    # type(df_column_rain_today)

    dataframe_to_parquet('ParquetFiles/df_today.parq', df_today)
    dataframe_to_parquet('ParquetFiles/df_tomorrow.parq', df_tomorrow)

    print(df_today.head())
    print(df_tomorrow.head())
