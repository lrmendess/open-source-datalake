from itertools import chain

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W

MONTH_ABBREV = {
    'JAN': 1,  'FEV': 2,  'MAR': 3,
    'ABR': 4,  'MAI': 5,  'JUN': 6,
    'JUL': 7,  'AGO': 8,  'SET': 9,
    'OUT': 10, 'NOV': 11, 'DEZ': 12
}

def filter_valid_rows(df):
    ''' Filter valid rows in excel sheet. '''
    month_abbrev_keys = list(MONTH_ABBREV.keys())
    df = df.where(F.col('month_abbrev').isin(month_abbrev_keys))
    return df

def fill_nullable_values(df):
    ''' Fills null values with the last non-null in the year column. '''
    df = df.withColumn('seq', F.monotonically_increasing_id())

    window = W.orderBy('seq').rowsBetween(W.unboundedPreceding, W.currentRow)
    previous_non_null_values = F.last('year', ignorenulls=True).over(window)
    df = df.withColumn('tmp_year', F.when(F.col('year').isNotNull(),
                                         F.col('year')).otherwise(previous_non_null_values))
    df = df.drop('seq', 'year')
    df = df.withColumnRenamed('tmp_year', 'year')

    return df

def map_number_of_months(df):
    ''' Maps the month abbreviation to the corresponding month number. '''
    month_abbrev_items = chain(*MONTH_ABBREV.items())
    month_abbrev_pairs = [F.lit(m) for m in month_abbrev_items]
    mapping_expr = F.create_map(month_abbrev_pairs)

    df = df.withColumn('month', mapping_expr[F.col('month_abbrev')])

    return df

# pylint: disable=C0116
def main():
    spark = (
        SparkSession.builder
        .appName('spark.raw.tb_ipca_hist')
        .enableHiveSupport()
        .getOrCreate()
    )

    df = (
        spark.read
        .format('com.crealytics.spark.excel')
        .option('header', False)
        .option('inferSchema', False)
        .option('dataAddress', '0!A8:H1000')
        .load('s3a://landing/ipca_hist/ipca_202309SerieHist.xls')
    )

    df = df.select(
        F.col('_c0').alias('year'),
        F.col('_c1').alias('month_abbrev'),
        F.col('_c2').alias('index_number'),
        F.col('_c3').alias('pct_month'),
        F.col('_c4').alias('pct_3_months'),
        F.col('_c5').alias('pct_6_months'),
        F.col('_c6').alias('pct_12_months'),
        F.col('_c7').alias('pct_year')
    )

    df = (
        df
        .transform(filter_valid_rows)
        .transform(fill_nullable_values)
        .transform(map_number_of_months)
        .drop('month_abbrev')
    )

    (
        df
        .write
        .format('parquet')
        .mode('overwrite')
        .saveAsTable('raw.tb_ipca_hist')
    )

if __name__ == '__main__':
    main()
