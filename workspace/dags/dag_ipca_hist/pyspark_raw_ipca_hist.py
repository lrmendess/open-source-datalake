import os

from pyspark.sql import SparkSession, DataFrame

BUCKET_LANDING = os.getenv('BUCKET_LANDING')

target: str = 'raw.tb_ipca_hist'
source: str = f's3a://{BUCKET_LANDING}/ipca/'

spark: SparkSession = (
    SparkSession.builder
    .enableHiveSupport()
    .getOrCreate()
)

df: DataFrame = spark.read.csv(source, sep='\t', header=True)

if not spark.catalog.tableExists(target):
    spark.catalog.createTable(target, schema=df.schema, source='parquet')

(
    df
    .write
    .format('parquet')
    .mode('overwrite')
    .insertInto(target)
)
