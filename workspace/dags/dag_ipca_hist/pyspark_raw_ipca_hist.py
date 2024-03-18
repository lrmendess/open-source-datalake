import os

from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame

BUCKET_DATALAKE_LANDING = os.getenv('BUCKET_DATALAKE_LANDING')


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(spark: SparkSession, source: str) -> DataFrame:
    schema = (
        StructType()
        .add('data', 'string')
        .add('indice_mes', 'string')
        .add('indice_acumulado_ano', 'string')
        .add('indice_acumulado_12_meses', 'string')
        .add('num_indice_acumulado_jan_1993', 'string')
    )

    return spark.read.csv(source, sep='\t', schema=schema, header=True)


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .saveAsTable(target, format='parquet')
    )


def handle(spark: SparkSession, source: str, target: str) -> None:
    load(extract(spark, source), target)


if __name__ == '__main__':
    source = f's3a://{BUCKET_DATALAKE_LANDING}/ipca/'
    target = 'raw.tb_ipca_hist'
    spark = spark_session()
    handle(spark, source, target)
