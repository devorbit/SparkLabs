from pyspark.sql import DataFrame

from sparklabs.batch.sales.dataframe import utils as dataframe_utils


def sql(query: str) -> DataFrame:
    dataframe_utils.register_temp_views()
    return dataframe_utils.spark.sql(query)
