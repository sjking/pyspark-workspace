from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
import logging

logger = logging.getLogger(__name__)


def sum_n_numbers(spark: SparkSession, n: int):
    data_frame = spark.range(n + 1)
    trans = data_frame.repartition(2)

    result = trans.agg(fun.sum("id")).collect()[0][0]

    expected = (n * (n + 1) // 2)

    if result == expected:
        logger.info(f"Success: {result}")
    else:
        logger.error(f"Expected {expected}, but got {result}")
