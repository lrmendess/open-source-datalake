from datetime import datetime
from typing import Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)
from pyspark.testing import assertDataFrameEqual


@pytest.fixture
def spark_session() -> Iterator[SparkSession]:
    yield SparkSession.builder.enableHiveSupport().getOrCreate()


def test_transformation_poder_compra(spark_session: SparkSession) -> None:
    from src.poder_compra.pyspark_refined_poder_compra import transform

    input_df_1 = spark_session.createDataFrame(
        data=[(1320.00, 10, 2023),
              (1320.00, 11, 2023),
              (1320.00, 12, 2023)],
        schema=StructType([
            StructField('vl_salario_minimo', DoubleType()),
            StructField('nu_mes', IntegerType()),
            StructField('nu_ano', IntegerType()),
        ])
    )

    input_df_2 = spark_session.createDataFrame(
        data=[(10, 2023, 'Belo Horizonte', 721.17),
              (11, 2023, 'Belo Horizonte', 728.27),
              (12, 2023, 'Belo Horizonte', 738.61)],
        schema=StructType([
            StructField('nu_mes', IntegerType()),
            StructField('nu_ano', IntegerType()),
            StructField('nm_uf', StringType(), False),
            StructField('vl_cesta_basica', DoubleType()),
        ])
    )

    input_df = {
        'salario_minimo': input_df_1,
        'preco_cesta_basica': input_df_2
    }

    expected_df = spark_session.createDataFrame(
        data=[(2023, 10, 1320.0, 'Belo Horizonte', 721.17, datetime(2023, 10, 31), 1.83),
              (2023, 11, 1320.0, 'Belo Horizonte', 728.27, datetime(2023, 11, 30), 1.81),
              (2023, 12, 1320.0, 'Belo Horizonte', 738.61, datetime(2023, 12, 31), 1.79)],
        schema=StructType([
            StructField('nu_ano', IntegerType()),
            StructField('nu_mes', IntegerType()),
            StructField('vl_salario_minimo', DoubleType()),
            StructField('nm_uf', StringType()),
            StructField('vl_cesta_basica', DoubleType()),
            StructField('dt_data', DateType()),
            StructField('vl_poder_compra', DoubleType())
        ])
    )

    # Act
    transformed_df = transform(input_df)

    # Assert
    assertDataFrameEqual(transformed_df, expected_df)
