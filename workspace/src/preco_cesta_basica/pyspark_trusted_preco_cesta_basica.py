import re

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession
from unicodedata import normalize


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(spark: SparkSession, source: str) -> DataFrame:
    return spark.read.table(source)


def sanitize_columns(df: DataFrame) -> DataFrame:
    regex = re.compile(r'\d+\s-\s.*\s-\s(.*)\s-\s+.*')
    for col in df.columns:
        renamed = col
        if match := regex.match(col):
            renamed = match.group(1)
        renamed = renamed.encode('ASCII', 'ignore').decode().strip().lower()
        df = df.withColumnRenamed(col, renamed)
    return df


def transform(df: DataFrame) -> DataFrame:
    df.transform(sanitize_columns)
    return df


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .saveAsTable(target, format='parquet')
    )


def handle(spark: SparkSession, source: str, target: str) -> None:
    load(transform(extract(spark, source)), target)


if __name__ == '__main__':
    source = 'raw.tb_preco_cesta_basica'
    target = 'trusted.tb_preco_cesta_basica'
    spark = spark_session()
    handle(spark, source, target)
