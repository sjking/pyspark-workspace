from pyspark.sql import SparkSession
import logging


logger = logging.getLogger(__name__)


def example1(spark: SparkSession):
    dataframe_1 = spark.range(10)
    dataframe_2 = spark.range(10)

    trans_1 = dataframe_1.repartition(20)
    trans_2 = dataframe_2.repartition(20)

    muls_of_3 = trans_1.selectExpr('id * 3 as id')

    join = muls_of_3.join(trans_2, "id")

    sum_ = join.selectExpr("sum(id)")

    result = sum_.collect()

    logger.info(result[0][0])
