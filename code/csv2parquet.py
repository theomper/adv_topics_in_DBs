from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("csv2parquet").getOrCreate()

movies_genres=spark.read.csv('hdfs://master:9000/files/movie_genres.csv')
movies_genres.write.parquet('hdfs://master:9000/files/movie_genres.parquet')

movies=spark.read.csv('hdfs://master:9000/files/movies.csv')
movies.write.parquet('hdfs://master:9000/files/movies.parquet')

ratings=spark.read.csv('hdfs://master:9000/files/ratings.csv')
ratings.write.parquet('hdfs://master:9000/files/ratings.parquet')