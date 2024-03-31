from typing import Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)
from pyspark.testing import assertDataFrameEqual


@pytest.fixture
def spark_session() -> Iterator[SparkSession]:
    yield SparkSession.builder.enableHiveSupport().getOrCreate()


def test_transformation_cesta_basica(spark_session: SparkSession) -> None:
    from src.preco_cesta_basica.pyspark_trusted_preco_cesta_basica import transform

    input_df = spark_session.createDataFrame(
        data=[('01/1998', '95,17 '),
              ('02/1998', '94,52 '),
              ('03/1998', '94,79 '),
              ('Fonte', 'Dieese')],
        schema=['Data', '7481 - Cesta básica - Belo Horizonte - u.m.c.'])

    expected_df = spark_session.createDataFrame(
        data=[(1, 1998, 'Belo Horizonte', 95.17),
              (2, 1998, 'Belo Horizonte', 94.52),
              (3, 1998, 'Belo Horizonte', 94.79)],
        schema=StructType([
            StructField('nu_mes', IntegerType()),
            StructField('nu_ano', IntegerType()),
            StructField('nm_uf', StringType(), False),
            StructField('vl_cesta_basica', DoubleType()),
        ])
    )

    # Act
    transformed_df = transform(input_df)

    # Assert
    assertDataFrameEqual(transformed_df, expected_df)
