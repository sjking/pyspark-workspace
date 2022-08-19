import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def get_spark_context(app_name: str) -> SparkSession:

    conf = SparkConf()

    conf.setAll(
        [
            ("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")),
            ("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "local[*]")),
            ("spark.submit.deployMode", "client"),
            ("spark.driver.bindAddress", os.environ.get("SPARK_BIND_ADDRESS", "0.0.0.0")),
            ("spark.app.name", app_name),
         ]
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()
