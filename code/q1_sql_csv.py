from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("query1-sql-csv").getOrCreate()

start_time = time.time()
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")

movies.registerTempTable("movies")

sqlString = \
        "SELECT (m._c1) AS Title, YEAR(m._c3) AS Year " + \
        "FROM movies AS m " + \
        "INNER JOIN (" + \
                "SELECT YEAR(mov._c3) AS Year, " + \
                        "MAX(((mov._c6)-(mov._c5))/(mov._c5)*100) AS Profit " + \
                "FROM movies AS mov "+ \
                "WHERE mov._c3 IS NOT NULL AND mov._c5 IS NOT NULL AND mov._c6 IS NOT NULL " + \
                        "AND YEAR(mov._c3)>=2000 AND mov._c5>0 AND mov._c6>0 "+ \
                "GROUP BY YEAR(mov._c3) " + \
        ") AS p " + \
        "ON Year = p.Year " + \
        "WHERE p.Profit = ((m._c6)-(m._c5))/(m._c5)*100 "+ \
        "ORDER BY Year "

res = spark.sql(sqlString)

res.show()

print("--- %s seconds ---" % (time.time() - start_time))