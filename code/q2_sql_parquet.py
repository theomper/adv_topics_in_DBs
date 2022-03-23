from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("query2-sql-parquet").getOrCreate()

start_time = time.time()
ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

ratings.registerTempTable("ratings")

sqlString = \
        "SELECT UsersOver3/Users*100 AS Percentage_of_Users_Over3 " + \
        "FROM " + \
                "(SELECT COUNT(DISTINCT _c0) AS Users " + \
                "FROM ratings AS r) " + \
                "CROSS JOIN " + \
                "( SELECT COUNT(*) AS UsersOver3 " + \
                "FROM " + \
                        "(SELECT _c0 " + \
                        "FROM ratings AS r " + \
                        "GROUP BY _c0 " + \
                        "HAVING AVG(_c2) > 3 ) )"

res = spark.sql(sqlString)

res.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs://master:9000/outputs/q2_parquet")

res.show()

print("--- %s seconds ---" % (time.time() - start_time))