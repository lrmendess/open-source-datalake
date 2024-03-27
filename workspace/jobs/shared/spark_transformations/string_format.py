import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame


def string_to_double(df: DataFrame,
                     thousand_sep: str = ',',
                     decimal_sep: str = '.',
                     *cols: str) -> DataFrame:
    for col in cols or []:
        df = df.withColumn(col, F.regexp_replace(col, fr'\{thousand_sep}', ''))
        df = df.withColumn(col, F.regexp_replace(col, fr'\{decimal_sep}', '.'))
        df = df.withColumn(col, F.col(col).cast('double'))
    return df
