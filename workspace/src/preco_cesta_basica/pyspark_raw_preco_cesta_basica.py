import os

from pyspark.sql import SparkSession, DataFrame

BUCKET_DATALAKE_LANDING = os.getenv('BUCKET_DATALAKE_LANDING')

spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

source = f's3a://{BUCKET_DATALAKE_LANDING}/preco-cesta-basica/'
target = 'raw.tb_preco_cesta_basica'

df: DataFrame = spark.read.csv(source, sep=';', header=True)

(
    df
    .write
    .mode('overwrite')
    .saveAsTable(target, format='parquet')
)
