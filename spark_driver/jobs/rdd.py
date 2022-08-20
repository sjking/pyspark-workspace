from pyspark.sql import SparkSession
import logging


logger = logging.getLogger(__name__)


def run_rdd_example(spark: SparkSession):
    ctx = spark.sparkContext
    brands = ["Tesla", "Ford", "GM"]
    brands_rdd = ctx.parallelize(brands, 5)

    logger.info(f"Num partitions: {brands_rdd.getNumPartitions()}")

    # From data sources
    data = ctx.textFile("file:///tmp/data/cars.data")

    logger.info(f"Num records: {data.count()}")

    # rdd from dataframes
    df = spark.range(100)
    rdd = df.rdd
    logger.info(f"RDD count: {rdd.count()}")

