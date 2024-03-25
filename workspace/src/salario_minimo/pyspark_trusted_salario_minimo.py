import re

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from slugify import slugify


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
        renamed = slugify(renamed, separator='_')
        df = df.withColumnRenamed(col, renamed)

    return df


def extract_date_and_year(df: DataFrame) -> DataFrame:
    df = df.withColumn('nu_mes', F.split('data', '/').getItem(0).cast('integer'))
    df = df.withColumn('nu_ano', F.split('data', '/').getItem(1).cast('integer'))
    return df


def remove_invalid_rows(df: DataFrame) -> DataFrame:
    return df.na.drop(how='any', subset=['nu_mes', 'nu_ano'])


def convert_br_str_to_double(df: DataFrame) -> DataFrame:
    for col in ['salario_minimo']:
        df = df.withColumn(col, F.regexp_replace(col, r'\.', ''))
        df = df.withColumn(col, F.regexp_replace(col, r',', '.'))
        df = df.withColumn(col, F.col(col).cast('double'))

    return df


def transform(df: DataFrame) -> DataFrame:
    df = (df
          .transform(sanitize_columns)
          .transform(extract_date_and_year)
          .transform(remove_invalid_rows)
          .transform(convert_br_str_to_double)
          .withColumnRenamed('salario_minimo', 'vl_salario_minimo')
          .drop('data'))

    return df


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .format('parquet')
        .partitionBy('nu_ano', 'nu_mes')
        .saveAsTable(target)
    )


def handle(spark: SparkSession, source: str, target: str) -> None:
    load(transform(extract(spark, source)), target)


if __name__ == '__main__':
    source = 'raw.tb_salario_minimo'
    target = 'trusted.tb_salario_minimo'
    spark = spark_session()
    handle(spark, source, target)
