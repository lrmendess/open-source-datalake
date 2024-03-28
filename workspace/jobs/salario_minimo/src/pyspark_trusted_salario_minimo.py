import re

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from slugify import slugify

from spark_transformations.string_format import string_to_double


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(spark: SparkSession, source: str) -> DataFrame:
    return spark.read.table(source)


def sanitize_columns(df: DataFrame) -> DataFrame:
    regex = re.compile(r'\d+\s-\s(.*)\s-\s+.*')
    for col in df.columns:
        renamed = str(col)
        if match := regex.match(col):
            renamed = match.group(1)
        df = df.withColumnRenamed(col, slugify(renamed, separator='_'))
    return df


def extract_date_and_year(df: DataFrame) -> DataFrame:
    df = df.withColumn('nu_mes', F.split('data', '/').getItem(0).cast('integer'))
    df = df.withColumn('nu_ano', F.split('data', '/').getItem(1).cast('integer'))
    return df


def transform(df: DataFrame) -> DataFrame:
    df = (df
          .transform(sanitize_columns)
          .transform(extract_date_and_year)
          .transform(string_to_double, '.', ',', 'salario_minimo')
          .withColumnRenamed('salario_minimo', 'vl_salario_minimo'))

    df = (df.na
          .drop(how='any', subset=['nu_mes', 'nu_ano'])
          .drop('data'))

    return df


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .format('parquet')
        .partitionBy('nu_ano')
        .saveAsTable(target)
    )


def handle(spark: SparkSession, source: str, target: str) -> None:
    load(transform(extract(spark, source)), target)


if __name__ == '__main__':
    source = 'raw.tb_salario_minimo'
    target = 'trusted.tb_salario_minimo'
    spark = spark_session()
    handle(spark, source, target)
