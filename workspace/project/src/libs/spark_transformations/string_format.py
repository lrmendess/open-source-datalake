import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame


def string_to_double(
    df: DataFrame,
    thousand_sep: str = ',',
    decimal_sep: str = '.',
    *cols: str
) -> DataFrame:
    """ Convert string to double in a Spark DataFrame.

    Args:
        df (DataFrame): Spark DataFrame.
        thousand_sep (str, optional): Thousand separator. Defaults to ','.
        decimal_sep (str, optional): Decimal separator. Defaults to '.'.
        *cols (str): Column names to be formatted.

    Returns:
        DataFrame: DataFrame with changed columns.
    """
    for col in cols or []:
        df = df.withColumn(col, F.regexp_replace(col, fr'\{thousand_sep}', ''))
        df = df.withColumn(col, F.regexp_replace(col, fr'\{decimal_sep}', '.'))
        df = df.withColumn(col, F.col(col).cast('double'))
    return df
