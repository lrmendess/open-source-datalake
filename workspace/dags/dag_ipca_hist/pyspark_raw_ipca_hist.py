import os

from pyspark.sql import SparkSession, DataFrame

BUCKET_DATALAKE_LANDING = os.getenv('BUCKET_DATALAKE_LANDING')

target: str = 'raw.tb_ipca_hist'
source: str = f's3a://{BUCKET_DATALAKE_LANDING}/ipca/'

spark: SparkSession = (
    SparkSession.builder
    .enableHiveSupport()
    .getOrCreate()
)

df: DataFrame = spark.read.csv(source, sep='\t', header=True)

(
    df
    .write
    .format('parquet')
    .mode('overwrite')
    .saveAsTable(target)
)
