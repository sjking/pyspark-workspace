import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


logger = logging.getLogger(__name__)


def quick_example(spark: SparkSession):
    """
    https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

    Run in terminal:

    nc -lk 9999
    hello world
    apache hadoop
    """

    # unbounded table containing streaming text data
    # one column of strings named "value"
    # Input table
    lines = spark.readStream\
        .format('socket')\
        .option('host', 'localhost')\
        .option('port', 9999)\
        .load()

    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias('word')
    )

    # streaming dataframe representing running word counts of stream
    # Result table
    word_counts = words.groupBy('word').count()

    # print complete set of counts (complete) to console every time its
    # updated
    query = word_counts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    logger.info(f"Started streaming")

    query.awaitTermination()

    logger.info(f"Ended streaming")
