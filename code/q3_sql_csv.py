from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("query3-sql-csv").getOrCreate()

start_time = time.time()
movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")

movie_genres.registerTempTable("movie_genres")
ratings.registerTempTable("ratings")

sqlString = \
        "SELECT Genres, AVG(Average_Rating) AS Final_Rating, COUNT(DISTINCT Movies) AS Genre_Movies " + \
        "FROM ( " + \
            "SELECT Ratings.Movies, Average_Rating, (mg._c1) AS Genres " + \
            "FROM movie_genres AS mg, ( " + \
                "SELECT _c1 AS Movies, AVG(_c2) AS Average_Rating " + \
                "FROM ratings " + \
                "GROUP BY _c1" + \
            ") AS Ratings " + \
            "WHERE mg._c0 = Ratings.Movies) " + \
        "GROUP BY Genres " + \
        "ORDER BY Genres ASC"

res = spark.sql(sqlString)

res.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs://master:9000/outputs/q3_csv")

res.show()

print("--- %s seconds ---" % (time.time() - start_time))