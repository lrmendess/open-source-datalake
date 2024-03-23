import os

from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame

BUCKET_DATALAKE_LANDING = os.getenv('BUCKET_DATALAKE_LANDING')

spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

source = f's3a://{BUCKET_DATALAKE_LANDING}/ipca/'
target = 'raw.tb_ipca_hist'

schema = (
    StructType()
    .add('data', 'string')
    .add('indice_mes', 'string')
    .add('indice_acumulado_ano', 'string')
    .add('indice_acumulado_12_meses', 'string')
    .add('num_indice_acumulado_jan_1993', 'string')
)

df: DataFrame = spark.read.csv(source, sep='\t', schema=schema, header=True)

(
    df
    .write
    .mode('overwrite')
    .saveAsTable(target, format='parquet')
)
