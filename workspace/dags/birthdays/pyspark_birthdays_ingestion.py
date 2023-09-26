from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName('spark.raw.birthdays')
    .enableHiveSupport()
    .getOrCreate()
)

df = (
    spark.read
    .option('header', True)
    .csv('s3a://landing/birthdays/birthdays.csv')
)

(
    df.write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable('raw.tb_birthdays')
)
