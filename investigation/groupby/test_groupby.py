import pandas as pd
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as f


def fn_pd():
    df = pd.read_csv('nba.csv')
    print(df.columns)
    print(df)
    print('=================================')

    grouped_df = df.groupby('Team').tail(10).set_index('Name')
    grouped_df.to_csv('grouped_team_nba.csv')


def fn_spark():
    spark = SparkSession.builder.appName('test_groupby').getOrCreate()
    df = spark.read.option('header', True).csv('nba.csv')
    window = Window.partitionBy('Team').orderBy('Name')
    df.withColumn('row_no', f.row_number().over(window)).show(truncate=False)


# fn_pd()
fn_spark()


