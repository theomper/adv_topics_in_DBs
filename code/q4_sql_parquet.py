from pyspark.sql import SparkSession
import time

def count_words(corpus):
    if corpus is not None:
        return len(corpus.split(' '))
    return 0

def periods(year):
    if (year <= 2004):
        return "2000-2004"
    elif (year <= 2009):
        return "2005-2009"
    elif (year <= 2014):
        return "2010-2014"
    elif (year <= 2019):
        return "2015-2019"

spark = SparkSession.builder.appName("query4-sql-parquet").getOrCreate()

start_time = time.time()
movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")

movie_genres.registerTempTable("movie_genres")
movies.registerTempTable("movies")

spark.udf.register("count_words", count_words)
spark.udf.register("periods", periods)

sqlString = \
        "SELECT SUM(count_words(m._c2))/count(m._c0) AS Length, " + \
        "periods(YEAR(m._c3)) AS Year " +\
        "FROM movies AS m, movie_genres AS mg " +\
        "WHERE m._c0 = mg._c0 AND mg._c1 = 'Drama' " + \
                "AND YEAR(m._c3) IS NOT NULL AND YEAR(m._c3) >= 2000 " + \
        "GROUP BY Year " + \
        "ORDER BY Year "

res = spark.sql(sqlString)

res.show()

print("--- %s seconds ---" % (time.time() - start_time))