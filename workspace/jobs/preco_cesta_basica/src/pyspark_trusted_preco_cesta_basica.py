import re

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from spark_transformations.string_format import string_to_double
from spark_transformations.null_values import fill_null_with_surrounding_avg

UF_MARKER = 'UF_'


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(spark: SparkSession, source: str) -> DataFrame:
    return spark.read.table(source)


def sanitize_columns(df: DataFrame) -> DataFrame:
    regex = re.compile(r'\d+\s-\s.*\s-\s(.*)\s-\s+.*')
    for col in df.columns:
        renamed = str(col)
        if match := regex.match(col):
            renamed = UF_MARKER + match.group(1)
        df = df.withColumnRenamed(col, renamed)
    return df


def extract_date_and_year(df: DataFrame) -> DataFrame:
    df = df.withColumn('nu_mes', F.split('data', '/').getItem(0).cast('integer'))
    df = df.withColumn('nu_ano', F.split('data', '/').getItem(1).cast('integer'))
    return df


def unpivot_uf_columns(df: DataFrame) -> DataFrame:
    ids = ['nu_mes', 'nu_ano']
    cols = [col for col in df.columns if col.startswith(UF_MARKER)]
    df = df.unpivot(ids, cols, 'nm_uf', 'vl_cesta_basica')
    return df.orderBy('nu_ano', 'nu_mes')


def transform(df: DataFrame) -> DataFrame:
    df = (df
          .transform(sanitize_columns)
          .transform(extract_date_and_year))

    df = df.na.drop(how='any', subset=['nu_mes', 'nu_ano'])

    df = (df
          .transform(unpivot_uf_columns)
          .transform(string_to_double, '.', ',', 'vl_cesta_basica')
          .withColumn('nm_uf', F.regexp_replace('nm_uf', UF_MARKER, '')))

    df = df.transform(
        func=fill_null_with_surrounding_avg,
        col='vl_cesta_basica',
        partitions=['nm_uf'],
        order_by=['nu_ano', 'nu_mes']
    )

    return df


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .format('parquet')
        .partitionBy('nm_uf', 'nu_ano')
        .saveAsTable(target)
    )


def handle(spark: SparkSession, source: str, target: str) -> None:
    load(transform(extract(spark, source)), target)


if __name__ == '__main__':
    spark = spark_session()
    source = 'raw.tb_preco_cesta_basica'
    target = 'trusted.tb_preco_cesta_basica'
    handle(spark, source, target)
