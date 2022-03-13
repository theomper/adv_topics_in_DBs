# code for capturing execution time copied from
# https://stackoverflow.com/questions/1557571/how-do-i-get-time-of-a-python-programs-execution

from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

# Given help function 
def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

# Start clock
start_time = time.time()

spark = SparkSession.builder.appName("q4_rdd").getOrCreate()
sc = spark.sparkContext

# Inputs - Movies, Genres and Ratings
# map => (movie_id, title, description, publish_date, duration, cost, income, favoured)
movies = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map(lambda x: split_complex(x))

# map => (movie_id, genre)
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x: split_complex(x))

# map => (user_id, movie_id, rating, timestamp)
ratings = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x: split_complex(x))

# Outputs
# for result in results.collect():
#     print(result)

# Print time spent for execution
print("---Completed in %s seconds ---" % (time.time() - start_time))