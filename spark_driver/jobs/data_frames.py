from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def gone_with_the_wind(spark: SparkSession):
    ratings = [
        ("Gone with the Wind", 6),
        ("Gone with the Wind", 8),
        ("Gone with the Wind", 8)
    ]

    data_df = spark.createDataFrame(ratings).toDF("movie", "rating")
    avg_df = data_df.groupby('movie').agg(avg('rating'))

    avg_df.show()
