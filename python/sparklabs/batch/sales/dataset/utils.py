from dataclasses import fields
from typing import Type, TypeVar

from pyspark.sql import DataFrame

from sparklabs.batch.sales.dataframe import utils as dataframe_utils

T = TypeVar("T")

spark = dataframe_utils.spark
customers = dataframe_utils.customers
deliveries = dataframe_utils.deliveries
events = dataframe_utils.events
inventory = dataframe_utils.inventory
order_items = dataframe_utils.order_items
orders = dataframe_utils.orders
products = dataframe_utils.products
promotions = dataframe_utils.promotions
returns = dataframe_utils.returns
sales_reps = dataframe_utils.sales_reps
stores = dataframe_utils.stores


def to_records(dataframe: DataFrame, model: Type[T]):
    names = [field.name for field in fields(model)]
    return dataframe.select(*names).rdd.map(lambda row: model(**row.asDict()))
