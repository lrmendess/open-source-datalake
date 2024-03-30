import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.testing import assertDataFrameEqual

sys.path.append('../')


@pytest.fixture
def spark_session() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def test_transformation_cesta_basica(spark_session: SparkSession) -> None:
    from src.pyspark_trusted_preco_cesta_basica import transform

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

    print(expected_df.schema)
    print(expected_df.show())

    # Act
    transformed_df = transform(input_df)

    print(transformed_df.schema)
    print(transformed_df.show())

    # Assert
    assertDataFrameEqual(transformed_df, expected_df)
