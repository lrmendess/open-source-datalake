from pyspark.sql import DataFrame, SparkSession


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(spark: SparkSession, source: str) -> DataFrame:
    return spark.read.table(source)


def transform(df: DataFrame) -> DataFrame:
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
    source = 'trusted.tb_ipca_hist'
    target = 'refined.tb_ipca_hist'
    spark = spark_session()
    handle(spark, source, target)
