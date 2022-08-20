from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def count_movie_sequel(spark: SparkSession):
    movies = spark.read.format('csv')\
        .option('header', True)\
        .option('inferSchema', True)\
        .load('/tmp/data/BollywoodMovieDetail.csv')

    num_sequels = movies.select('sequel')\
        .where(col('sequel') == '1')\
        .agg(count('sequel'))\
        .alias('Number of sequels produced')

    # action causes evaluation of previous transformations
    num_sequels.show()

