import re

from pyspark.sql import DataFrame, SparkSession
from slugify import slugify


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(spark: SparkSession, source: str) -> DataFrame:
    return spark.read.table(source)


def sanitize_columns(df: DataFrame) -> DataFrame:
    regex = re.compile(r'\d+\s-\s.*\s-\s(.*)\s-\s+.*')
    for col in df.columns:
        renamed = str(col)
        if match := regex.match(col):
            renamed = match.group(1)
        renamed = slugify(renamed, separator='_')
        df = df.withColumnRenamed(col, renamed)
    return df


def transform(df: DataFrame) -> DataFrame:
    df = df.transform(sanitize_columns)
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
