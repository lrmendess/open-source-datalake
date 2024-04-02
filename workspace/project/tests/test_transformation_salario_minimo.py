from typing import Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType
from pyspark.testing import assertDataFrameEqual


@pytest.fixture
def spark_session() -> Iterator[SparkSession]:
    yield SparkSession.builder.enableHiveSupport().getOrCreate()


def test_transformation_salario_minimo(spark_session: SparkSession) -> None:
    from src.salario_minimo.pyspark_trusted_salario_minimo import transform

    input_df = spark_session.createDataFrame(
        data=[('10/2023', '1.320,00'),
              ('11/2023', '1.320,00'),
              ('12/2023', '1.320,00'),
              ('Fonte', 'MTb')],
        schema=['Data', '1619 - Salário mínimo - u.m.c.'])

    expected_df = spark_session.createDataFrame(
        data=[(1320.00, 10, 2023),
              (1320.00, 11, 2023),
              (1320.00, 12, 2023)],
        schema=StructType([
            StructField('vl_salario_minimo', DoubleType()),
            StructField('nu_mes', IntegerType()),
            StructField('nu_ano', IntegerType()),
        ])
    )

    # Act
    transformed_df = transform(input_df)

    # Assert
    assertDataFrameEqual(transformed_df, expected_df)
