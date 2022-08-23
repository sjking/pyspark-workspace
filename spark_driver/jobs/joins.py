from pyspark.sql import SparkSession
import pyspark.sql.functions as fun


def joins(spark: SparkSession):
    movies = spark.read.format('csv') \
        .option('header', True) \
        .option('samplingRatio', 0.001) \
        .option('inferSchema', True) \
        .load('/tmp/data/BollywoodMovieDetail.csv')

    movies.createOrReplaceTempView('movies_temp')

    actors = spark.read.format('csv')\
        .option('header', True) \
        .option('samplingRatio', 0.001) \
        .option('inferSchema', True) \
        .load('/tmp/data/BollywoodActorRanking.csv')

    actors.createOrReplaceTempView('actors_temp')

    query = """
    SELECT title, releaseYear, hitFlop, split(actors,"[|]") AS actors FROM movies_temp
    """
    spark.sql(query).createOrReplaceTempView('temp_table_1')

    query = """
    SELECT title, releaseYear, hitFlop, upper(trim(actor)) AS actor FROM
    (SELECT title, releaseYear, hitFlop, explode(actors) AS actor FROM temp_table_1)
    """
    spark.sql(query).createOrReplaceTempView('temp_table_2')

    actors_df = actors.withColumn('actorName', fun.trim(fun.upper(actors.actorName)))
    movies_df = spark.sql('SELECT * FROM temp_table_2')

    movies_df.join(actors_df, movies_df.actor == actors_df.actorName).show(5, False)

    # union
    df1 = movies.select('title', 'releaseYear').where(movies.releaseYear > 2010).limit(2)
    df2 = movies.select('title', 'releaseYear').where(movies.releaseYear < 2010).limit(2)

    df1.union(df2).show()

    # windowing functions
    query = """
    SELECT title, hitFlop, releaseYear, dense_rank() OVER
    (PARTITION BY releaseYear ORDER BY hitFlop DESC) as rank
    FROM movies_temp WHERE releaseYear=2013
    """
    spark.sql(query).show(3)

    query = """
    SELECT * FROM (SELECT title, hitFlop, releaseYear, dense_rank() OVER (PARTITION BY releaseYear ORDER BY hitFLop DESC) AS rank  FROM movies_temp) 
    tmp WHERE rank <=2 ORDER BY releaseYear
    """
    spark.sql(query).show(7)



