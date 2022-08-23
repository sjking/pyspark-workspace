from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    expr,
    to_date,
    year
)
import pyspark.sql.functions as fun
import logging

logger = logging.getLogger(__name__)


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

    # Specify schema using DDL
    ddl = ",".join(["imdbId STRING",
                    "title STRING",
                    "releaseYear DATE",
                    "releaseDate STRING",
                    "genre STRING",
                    "writers STRING",
                    "actors STRING",
                    "directors STRING",
                    "sequel INT",
                    "hitFlop INT"])

    movies = spark.read.format('csv')\
        .option('header', True)\
        .option('inferSchema', False)\
        .schema(ddl).load('/tmp/data/BollywoodMovieDetail.csv')
    movies.show(1, False)

    # save as parquet: HDFS helps
    # movies.write.format('csv').save('/tmp/moviesFile')

    logger.info(f"Columns: {movies.columns}")

    rating_col = movies['hitFlop']
    movies.select(rating_col).show(5)
    movies.select(expr('hitFlop')).show(5)

    movies.select(expr('hitFlop > 5')).show(3)
    movies.select(rating_col > 5).show(3)

    movies.select(expr('hitFlop > 5').alias('Good ones')).show(2)

    # sorting columns
    movies.sort(rating_col.desc()).show(3)

    # Rows
    row = Row("Upcoming new Movie", 2021, "Comedy")
    logger.info(f"{row[0]}, {row[1]}, {row[2]}")

    rows = [("Tom Cruise Movie", 2021, "Comedy"), ("Rajinikanth Movie", 2021, "Action")]

    new_movies = spark.createDataFrame(rows).toDF("Movie name", "Release year", "Genre")
    new_movies.show()

    logger.info(f"Projections and filter")

    # projection and filter
    movies.select('title').where(col('hitFlop') > 8).show()
    movies.select('title').where(col('hitFlop') > 8).count()

    num_romance = movies.select('title').filter(movies.genre.contains('Romance')).count()
    logger.info(f"Romance: {num_romance}")

    num_2010_romance = movies.select('title')\
        .filter(movies.genre.contains('Romance'))\
        .where(movies.releaseYear > '2010').count()

    logger.info(f"2010 Romance: {num_2010_romance}")

    movies.select('releaseYear').distinct().sort(movies.releaseYear.desc()).show()

    # Changing column names
    movies_renamed = movies.withColumnRenamed('hitFlop', 'rating')
    movies_renamed.printSchema()

    # Change column types
    movies_launched = movies.withColumn('launchDate', to_date(movies.releaseDate, "d MMM yyyy"))\
        .drop('releaseDate')

    movies_launched.printSchema()

    movies_launched = movies.withColumn('launchDate', to_date(movies.releaseDate, "d MMM yyyy"))
    # Check for rows where conversion failed
    movies_launched.select('releaseDate', 'launchDate')\
        .where(movies_launched.launchDate.isNull())\
        .show(5, False)

    num_failed_conversion = movies_launched.select('releaseDate', 'launchDate')\
        .where(movies_launched.launchDate.isNull()).count()

    logger.info(f"Failed conversion: {num_failed_conversion}")

    movies_launched.select(year('launchDate')).distinct().orderBy(year('launchDate')).show()

    # Aggregations
    movies.select('releaseYear').groupby('releaseYear').count().orderBy('releaseYear').show()

    # max value in rating
    movies_renamed.select(fun.max('rating')).show()
    movies_renamed.select(fun.min('rating')).show()

    movies_renamed.select(fun.sum('rating')).show()
    movies_renamed.select(fun.avg('rating')).show()

    # Average rating for movies released in each year
    movies_renamed.select('releaseYear', 'rating')\
        .groupBy('releaseYear')\
        .avg('rating')\
        .orderBy('releaseYear')\
        .show()


