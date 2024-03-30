from typing import List

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.dataframe import DataFrame


def fill_null_with_surrounding_avg(
    df: DataFrame,
    partitions: List[str],
    order_by: List[str],
    col: str
) -> DataFrame:
    """ Fill numeric and null values with the average of the values from the previous and next row.
    These values used are obtained through the partitioning and ordering of a WindowSpec.

    Args:
        df (DataFrame): Spark DataFrame
        partitions (List[str]): Columns to partition the values to be sorted.
        order_by (List[str]): Columns to order the partitioned values.
        col (str): Name of the column to be filled.

    Returns:
        DataFrame: DataFrame with filled column.
    """
    w1 = W.partitionBy(partitions) \
          .orderBy(order_by) \
          .rowsBetween(W.unboundedPreceding, W.currentRow)

    w2 = W.partitionBy(partitions) \
          .orderBy(order_by) \
          .rowsBetween(W.currentRow, W.unboundedFollowing)

    prev_col = f'prev_{col}'
    next_col = f'next_{col}'

    avg_col = (F.coalesce(prev_col, next_col) + F.coalesce(next_col, prev_col)) / 2
    df = df.withColumn(prev_col, F.last(col, ignorenulls=True).over(w1))
    df = df.withColumn(next_col, F.first(col, ignorenulls=True).over(w2))
    df = df.withColumn(col, avg_col)

    return df.drop(prev_col, next_col)
