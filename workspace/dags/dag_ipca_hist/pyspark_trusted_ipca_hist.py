from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

month_abbrev = {
    'Jan': 1,  'Fev': 2,  'Mar': 3,
    'Abr': 4,  'Mai': 5,  'Jun': 6,
    'Jul': 7,  'Ago': 8,  'Set': 9,
    'Out': 10, 'Nov': 11, 'Dez': 12,
}


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def split_date_into_month_and_year(df: DataFrame) -> DataFrame:
    return df.withColumn('mes', F.split('data', '/').getItem(0)) \
             .withColumn('ano', F.split('data', '/').getItem(1).cast('integer'))


def map_month_name_to_number(df: DataFrame) -> DataFrame:
    month_abbrev_items = chain(*month_abbrev.items())
    month_abbrev_pairs = [F.lit(m) for m in month_abbrev_items]
    mapping_expr = F.create_map(month_abbrev_pairs)
    return df.withColumn('num_mes', mapping_expr[F.col('mes')])


def localize_number_to_br_standard(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn(col, F.regexp_replace(col, r'\.', '')) \
             .withColumn(col, F.regexp_replace(col, r',', '.')) \
             .withColumn(col, F.col(col).cast('double'))


def extract(spark: SparkSession, source: str) -> DataFrame:
    return spark.read.table(source)


def transform(df: DataFrame) -> DataFrame:
    df = df.transform(split_date_into_month_and_year) \
           .transform(map_month_name_to_number)

    df = df.transform(localize_number_to_br_standard, 'indice_mes') \
           .transform(localize_number_to_br_standard, 'indice_acumulado_ano') \
           .transform(localize_number_to_br_standard, 'indice_acumulado_12_meses') \
           .transform(localize_number_to_br_standard, 'num_indice_acumulado_jan_1993')

    df = df.select(
        F.col('ano').alias('num_ano'),
        F.col('num_mes').alias('num_mes'),
        F.col('indice_mes').alias('pct_mes'),
        F.col('indice_acumulado_ano').alias('pct_acum_ano'),
        F.col('indice_acumulado_12_meses').alias('pct_acum_12m'),
        F.col('num_indice_acumulado_jan_1993').alias('num_indice_acum_jan_93')
    )

    return df


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .partitionBy('num_ano')
        .saveAsTable(target, format='parquet')
    )


def handle(spark: SparkSession, source: str, target: str) -> None:
    load(transform(extract(spark, source)), target)


if __name__ == '__main__':
    source = 'raw.tb_ipca_hist'
    target = 'trusted.tb_ipca_hist'
    spark = spark_session()
    handle(spark, source, target)
