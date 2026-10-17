import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType

INPUT_PATH = "/opt/airflow/data/data_*.parquet"
INTERMEDIATE_PATH = "/opt/airflow/data/combined_parquet"
OUTPUT_PATH = "/opt/airflow/data/cleaned_parquet"


def getting_parquet():
    spark = (
        SparkSession.builder
        .appName("ReadParquetBatches")
        .getOrCreate()
    )

    try:
        file_paths = glob.glob(INPUT_PATH)

        if not file_paths:
            raise FileNotFoundError(
                f"No Parquet files found: {INPUT_PATH}"
            )

        df = spark.read.parquet(*file_paths)

        df.write.mode("overwrite").parquet(INTERMEDIATE_PATH)

        print(f"Read {len(file_paths)} Parquet files")
        print(f"Saved combined data to {INTERMEDIATE_PATH}")

    finally:
        spark.stop()


def cleaning_data():
    spark = (
        SparkSession.builder
        .appName("CleanParquetData")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(INTERMEDIATE_PATH)

        rename_cols = [
            col(c).alias(c.strip().lower().replace(" ", "_"))
            for c in df.columns
        ]

        df = df.select(*rename_cols)

        df_cleaned = df.withColumn(
            "symbol",
            col("symbol").cast(StringType())
        )

        numeric_cols = ["price", "high", "low"]

        for column_name in numeric_cols:
            df_cleaned = df_cleaned.withColumn(
                column_name + "_rounded",
                F.round(col(column_name), 2).cast(DoubleType())
            )

        df_cleaned = df_cleaned.drop(*numeric_cols)

        fill_dict = {
            f"{column_name}_rounded": df_cleaned.select(
                F.mean(f"{column_name}_rounded")
            ).first()[0]
            for column_name in numeric_cols
        }

        df_cleaned = df_cleaned.fillna(fill_dict)

        df_cleaned.createOrReplaceTempView("df_cleaned")

        order_desc = spark.sql("""
            SELECT
                symbol,
                price_rounded,
                high_rounded,
                low_rounded,
                RANK() OVER (
                    PARTITION BY symbol
                    ORDER BY price_rounded DESC
                ) AS price_rank
            FROM df_cleaned
        """)

        mean_symbol = spark.sql("""
            WITH avg_price AS (
                SELECT
                    symbol,
                    ROUND(AVG(price_rounded), 2) AS avg_price
                FROM df_cleaned
                GROUP BY symbol
            )
            SELECT
                c.symbol,
                a.avg_price,
                c.price_rounded,
                c.high_rounded,
                c.low_rounded,
                ROUND(
                    c.price_rounded - a.avg_price, 2
                ) AS price_deviation
            FROM df_cleaned c
            JOIN avg_price a ON c.symbol = a.symbol
        """)

        order_desc.show(10)
        mean_symbol.show(10)

        mean_symbol.write.mode("overwrite").parquet(OUTPUT_PATH)


    finally:
        spark.stop()