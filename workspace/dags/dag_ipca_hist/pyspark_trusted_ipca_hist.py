from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

source: str = 'raw.tb_ipca_hist'
target: str = 'trusted.tb_ipca_hist'

spark: SparkSession = (
    SparkSession.builder
    .enableHiveSupport()
    .getOrCreate()
)

month_abbrev = {
    'Jan': 1,  'Fev': 2,  'Mar': 3,
    'Abr': 4,  'Mai': 5,  'Jun': 6,
    'Jul': 7,  'Ago': 8,  'Set': 9,
    'Out': 10, 'Nov': 11, 'Dez': 12,
}


def split_date_into_month_and_year(df: DataFrame) -> DataFrame:
    return df.withColumn('mes', F.split('data', '/').getItem(0)) \
             .withColumn('ano', F.split('data', '/').getItem(1).cast('integer')) \
             .drop('data')


def map_month_name_to_number(df: DataFrame) -> DataFrame:
    month_abbrev_items = chain(*month_abbrev.items())
    month_abbrev_pairs = [F.lit(m) for m in month_abbrev_items]
    mapping_expr = F.create_map(month_abbrev_pairs)
    return df.withColumn('num_mes', mapping_expr[F.col('mes')])


def localize_number_to_br_standard(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn(col, F.regexp_replace(col, r'\.', '')) \
             .withColumn(col, F.regexp_replace(col, r',', '.')) \
             .withColumn(col, F.col(col).cast('double'))


df: DataFrame = spark.read.table(source)

df = df.transform(split_date_into_month_and_year) \
       .transform(map_month_name_to_number)

df = df.transform(localize_number_to_br_standard, 'indice_mes') \
       .transform(localize_number_to_br_standard, 'indice_acumulado_ano') \
       .transform(localize_number_to_br_standard, 'indice_acumulado_12_meses') \
       .transform(localize_number_to_br_standard, 'num_indice_acumulado_jan_1993')

(
    df
    .write
    .mode('overwrite')
    .partitionBy('ano')
    .saveAsTable(target, format='parquet')
)
