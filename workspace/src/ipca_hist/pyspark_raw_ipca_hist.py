import os

from pyspark.sql import SparkSession, DataFrame

BUCKET_DATALAKE_LANDING = os.getenv('BUCKET_DATALAKE_LANDING')

spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

source = f's3a://{BUCKET_DATALAKE_LANDING}/ipca/'
target = 'raw.tb_ipca_hist'

df: DataFrame = spark.read.csv(source, sep='\t', header=True)

(
    df
    .write
    .mode('overwrite')
    .saveAsTable(target, format='parquet')
)
