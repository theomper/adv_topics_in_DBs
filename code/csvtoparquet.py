from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csvtoparquet").getOrCreate()

df = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")
df.write.parquet("hdfs://master:9000/files/movies.parquet")

df = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
df.write.parquet("hdfs://master:9000/files/movie_genres.parquet")

df = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
df.write.parquet("hdfs://master:9000/files/ratings.parquet")