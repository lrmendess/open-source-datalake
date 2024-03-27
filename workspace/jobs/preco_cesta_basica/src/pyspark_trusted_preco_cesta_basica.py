import re
from typing import List

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql import DataFrame, SparkSession

from spark_transformations.string_format import string_to_double

UF_MARKER = 'UF_'


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(spark: SparkSession, source: str) -> DataFrame:
    return spark.read.table(source)


def get_columns_with_uf_marker(df: DataFrame) -> List[str]:
    return [col for col in df.columns if col.startswith(UF_MARKER)]


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


def remove_invalid_rows(df: DataFrame) -> DataFrame:
    return df.na.drop(how='any', subset=['nu_mes', 'nu_ano'])


def unpivot_uf_columns(df: DataFrame) -> DataFrame:
    cols = get_columns_with_uf_marker(df)

    df = df.unpivot(
        ids=['nu_mes', 'nu_ano'],
        values=cols,
        variableColumnName='nm_uf',
        valueColumnName='vl_cesta_basica'
    )

    return df.orderBy('nu_ano', 'nu_mes')


def remove_uf_marker(df: DataFrame) -> DataFrame:
    col_substr = F.substr('nm_uf', F.lit(len(UF_MARKER) + 1))
    return df.withColumn('nm_uf', col_substr)


def fill_null_with_surrounding_avg(df: DataFrame) -> DataFrame:
    w1 = W.partitionBy('nm_uf') \
          .orderBy('nu_ano', 'nu_mes') \
          .rowsBetween(W.unboundedPreceding, W.currentRow)

    w2 = W.partitionBy('nm_uf') \
          .orderBy('nu_ano', 'nu_mes') \
          .rowsBetween(W.currentRow, W.unboundedFollowing)

    avg_col = (F.coalesce('prev', 'next') + F.coalesce('next', 'prev')) / 2
    df = df.withColumn('prev', F.last('vl_cesta_basica', ignorenulls=True).over(w1))
    df = df.withColumn('next', F.first('vl_cesta_basica', ignorenulls=True).over(w2))
    df = df.withColumn('vl_cesta_basica', avg_col)
    df = df.drop('prev', 'next')

    return df


def transform(df: DataFrame) -> DataFrame:
    uf_cols = get_columns_with_uf_marker(df)

    df = (df
          .transform(sanitize_columns)
          .transform(extract_date_and_year)
          .transform(remove_invalid_rows)
          .transform(string_to_double, '.', ',', *uf_cols)
          .transform(unpivot_uf_columns)
          .transform(remove_uf_marker)
          .transform(fill_null_with_surrounding_avg))

    return df


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .format('parquet')
        .partitionBy('nu_ano', 'nu_mes', 'nm_uf')
        .saveAsTable(target)
    )


def handle(spark: SparkSession, source: str, target: str) -> None:
    load(transform(extract(spark, source)), target)


if __name__ == '__main__':
    source = 'raw.tb_preco_cesta_basica'
    target = 'trusted.tb_preco_cesta_basica'
    spark = spark_session()
    handle(spark, source, target)
