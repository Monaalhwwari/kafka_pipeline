from pyspark.sql import SparkSession
from pandas import *
import glob
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, coalesce, col, concat_ws, lit, md5
from pyspark.sql.types import DoubleType, IntegerType, StringType
import os
import sys


def getting_parquet():

    os.environ["HADOOP_HOME"] = "C:\\hadoop"

    spark = SparkSession \
        .builder \
        .appName("ReadParquetBatches") \
        .getOrCreate()

    file_paths = glob.glob("D:/fake_commerce_api/data_*.parquet")

    formatted_paths = [p.replace("\\", "/") for p in file_paths]

    if formatted_paths:
        df = spark.read.parquet(*formatted_paths)
    else:
        print("none")

    ### validating data
def cleaning_data():
    rename_cols = [col(c).alias(c.strip().lower().replace(" ", "_")) for c in df.columns]

    df = df.select(rename_cols)

    df_cleaned = df.withColumn("symbol",col("symbol").cast(StringType()))

    int_cols = ["price","high","low"]


    for col in int_cols:
        df_cleaned = df_cleaned.withColumn(col+"_rounded",F.round(df_cleaned[col], 2).cast(DoubleType()))
        df_cleaned = df_cleaned.drop(col)


    mean_price = df_cleaned.select(F.mean("price_rounded")).collect()[0][0]
    mean_high = df_cleaned.select(F.mean("high_rounded")).collect()[0][0]
    mean_low = df_cleaned.select(F.mean("low_rounded")).collect()[0][0]

    fill_dict = {
                "price_rounded": mean_price,
                "high_rounded": mean_high,
                "low_rounded": mean_low,
            }

    df_cleaned = df_cleaned.fillna(fill_dict)

    ### sql

    df_cleaned.createOrReplaceTempView("df_cleaned")

    order_desc = spark.sql("""
        SELECT 
            symbol,
            price_rounded,
            high_rounded,
            low_rounded,
            RANK() OVER (PARTITION BY symbol ORDER BY price_rounded DESC) AS price_rank
        FROM df_cleaned
        """)

    mean_symbol = spark.sql("""
        WITH  avg_price AS(
            SELECT symbol,
                ROUND(avg(price_rounded),2) AS avg_price
            FROM df_cleaned
            GROUP BY symbol
                )
        SELECT 
            c.symbol,
            a.avg_price,
            c.price_rounded,
            c.high_rounded,
            c.low_rounded,
            ROUND((c.price_rounded - a.avg_price),2) AS price_deviation
        FROM df_cleaned c
        JOIN avg_price a ON c.symbol = a.symbol
        """)

    mean_symbol.show(10)