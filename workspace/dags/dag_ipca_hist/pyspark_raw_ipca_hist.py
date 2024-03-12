from pyspark.sql import SparkSession, DataFrame

spark: SparkSession = (
    SparkSession.builder
    .appName('spark.raw.tb_ipca')
    .enableHiveSupport()
    .getOrCreate()
)

df: DataFrame = spark.read.csv('s3a://datalake-landing/ipca/', sep='\t', header=True)

if not spark.catalog.tableExists('raw.tb_ipca'):
    spark.catalog.createTable('raw.tb_ipca', schema=df.schema, source='parquet')

(
    df
    .write
    .format('parquet')
    .mode('overwrite')
    .insertInto('raw.tb_ipca')
)
