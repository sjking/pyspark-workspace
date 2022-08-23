from pyspark.sql import SparkSession


def query_example(spark: SparkSession):
    movies = spark.read.format('csv') \
        .option('header', True) \
        .option('samplingRatio', 0.001)\
        .option('inferSchema', True) \
        .load('/tmp/data/BollywoodMovieDetail.csv')

    movies.createOrReplaceTempView('tempView')

    spark.sql('SELECT title FROM tempView WHERE releaseYear > 2010 ORDER BY title desc')\
        .show(3)

    query = """
    SELECT title, CASE
    WHEN hitFlop < 5 THEN 'below average'
    WHEN hitFlop = 5 THEN 'average'
    WHEN hitFlop > 5 THEN 'above average' END as MovieRating
    FROM tempView WHERE releaseYear > 2010 ORDER BY title desc
    """

    spark.sql(query).show(10)

    # Creating tables
    spark.sql('CREATE DATABASE spark_course')
    spark.sql('USE spark_course')
    spark.sql('SHOW TABLES').show(5, False)

    # Need to have Hive support
    # spark.sql('CREATE TABLE movieShortDetail(imdbID String, title String)')
    # spark.sql('SHOW TABLES').show(5, False)

    query = """
    SELECT title, releaseYear, hitFlop, split(actors, "[|]") 
    AS actors FROM tempView
    """

    spark.sql(query).show(5, False)
    spark.sql(query).createOrReplaceTempView('tempMovieActorsView')

    spark.sql('SELECT title, releaseYear, hitFlop, explode(actors) AS actor FROM tempMovieActorsView LIMIT 10')\
        .createOrReplaceTempView('tempExplodeActorsView')\

    spark.sql('SELECT * from tempExplodeActorsView').show()

    query = """
    SELECT actor, avg(hitFlop) AS score FROM tempExplodeActorsView 
    GROUP BY actor ORDER BY score desc LIMIT 5
    """

    spark.sql(query).show()

    query = """
    SELECT explode(array_distinct(allTitles)) FROM 
    (SELECT collect_list(title) AS allTitles FROM tempExplodeActorsView)
    """

    spark.sql(query).show()


