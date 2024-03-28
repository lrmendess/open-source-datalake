import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def extract(
    spark: SparkSession,
    source_salario_minimo: str,
    source_preco_cesta_basica: str
) -> dict[str, DataFrame]:
    dfs = {'salario_minimo': spark.read.table(source_salario_minimo),
           'preco_cesta_basica': spark.read.table(source_preco_cesta_basica)}
    return dfs


def transform(dfs: dict[str, DataFrame]) -> DataFrame:
    df_salario_minimo = dfs['salario_minimo']
    df_preco_cesta_basica = dfs['preco_cesta_basica']

    df = df_salario_minimo.join(df_preco_cesta_basica, how='inner', on=['nu_ano', 'nu_mes'])
    col_poder_compra = F.format_number(F.col('vl_salario_minimo') / F.col('vl_cesta_basica'), 2)
    df = df.withColumn('vl_poder_compra', col_poder_compra)

    return df


def load(df: DataFrame, target: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .format('parquet')
        .partitionBy('nm_uf', 'nu_ano')
        .saveAsTable(target)
    )


def handle(
    spark: SparkSession,
    source_salario_minimo: str,
    source_preco_cesta_basica: str,
    target: str
) -> None:
    load(transform(extract(spark, source_salario_minimo, source_preco_cesta_basica)), target)


if __name__ == '__main__':
    spark = spark_session()
    source_salario_minimo = 'trusted.tb_salario_minimo'
    source_preco_cesta_basica = 'trusted.tb_preco_cesta_basica'
    target = 'refined.tb_poder_compra'
    handle(spark, source_salario_minimo, source_preco_cesta_basica, target)
