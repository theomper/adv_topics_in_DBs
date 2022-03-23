from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("query5-sql-csv").getOrCreate()

start_time = time.time()

movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")
ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")

movie_genres.registerTempTable("movie_genres")
movies.registerTempTable("movies")
ratings.registerTempTable("ratings")

sqlString = \
        "SELECT Genre_Stats.Genre AS Genre, first(Genre_Stats.User) AS User, " + \
            "first(Genre_Stats.Ratings) AS Ratings, first(Max_Popularity.Title) AS Favorite_Movie, " + \
            "first(Max_rate) AS Max_Rating, first(Min_Popularity.Title) AS Least_Favorite_Movie, " + \
            "first(Min_rate) AS Min_Rating " + \
        "FROM " + \
            "(SELECT Max_Min.Genre AS Genre, Max_Min.User AS User, " + \
                "Max_Count AS Ratings, Max_rate, Min_rate " + \
            \
            "FROM " + \
                "(SELECT MAX(Count_of_Ratings) AS Max_Count, " + \
                    "Genre " + \
                        "FROM " + \
                            "(SELECT r._c0 AS User, mg._c1 AS Genre, " + \
                                "COUNT(r._c0) AS Count_of_Ratings " + \
                            "FROM ratings AS r, movie_genres AS mg " + \
                            "WHERE r._c1 = mg._c0 " + \
                            "GROUP BY mg._c1, r._c0 " + \
                            ") AS Counts " + \
                        "GROUP BY Genre) AS Max_Genre, " + \
                    "(SELECT r._c0 AS User, mg._c1 AS Genre, " + \
                        "COUNT(r._c0) AS Count_of_Ratings, MAX(r._c2) AS Max_rate, " + \
                        "MIN(r._c2) AS Min_rate " + \
                    "FROM ratings AS r, movie_genres AS mg " + \
                    "WHERE r._c1 = mg._c0 " + \
                    "GROUP BY mg._c1, r._c0 " + \
                    ") AS Max_Min " + \
                "WHERE Max_Min.Count_of_Ratings = Max_Genre.Max_Count AND " +\
                    "Max_Min.Genre = Max_Genre.Genre " + \
                "ORDER BY Genre ASC) AS Genre_Stats, " + \
                \
                "(SELECT r._c0 AS User, r._c1 AS Movie, mg._c1 AS Genre, " + \
                    "m._c1 AS Title, r._c2 AS Rating, m._c7 AS Popularity " + \
                "FROM ratings AS r, movies AS m, movie_genres AS mg " + \
                "WHERE r._c1 = m._c0 AND m._c0 = mg._c0 " + \
                "ORDER BY User DESC, Rating DESC, Popularity DESC) AS Max_Popularity, " + \
                \
                "(SELECT r._c0 AS User, r._c1 AS Movie, mg._c1 AS Genre, " + \
                    "m._c1 AS Title, r._c2 AS Rating, m._c7 AS Popularity " + \
                "FROM ratings AS r, movies AS m, movie_genres AS mg " + \
                "WHERE r._c1 = m._c0 AND m._c0 = mg._c0 " + \
                "ORDER BY User DESC, Rating DESC, Popularity DESC) AS Min_Popularity " + \
                \
        "WHERE Genre_Stats.User = Max_Popularity.User AND Genre_Stats.User = Min_Popularity.User AND "+ \
            "Genre_Stats.Max_rate = Max_Popularity.Rating AND Genre_Stats.Min_rate = Min_Popularity.Rating " + \
            "AND Genre_Stats.Genre = Max_Popularity.Genre AND Genre_Stats.Genre = Min_Popularity.Genre " + \
        "GROUP BY Genre_Stats.Genre " + \
        "ORDER BY Genre_Stats.Genre ASC "

res = spark.sql(sqlString)

res.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs://master:9000/outputs/q5_csv")

res.show()

print("--- %s seconds ---" % (time.time() - start_time))