# code for capturing execution time copied from
# https://stackoverflow.com/questions/1557571/how-do-i-get-time-of-a-python-programs-execution

from re import L
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
# map => (movie_id, (title, favoured))
movies = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map(lambda x: split_complex(x)). \
    map(lambda x: (x[0], (x[1], float(x[7]))))

# map => (movie_id, genre)
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x: split_complex(x))

# map => (user_id, movie_id, rating, timestamp)
# map => (movie_id, (user_id, rating))
ratings = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x: split_complex(x)). \
    map(lambda x: (x[1], (x[0], float(x[2]))))

# Joins
# join => (movie_id, (genre, (title, favoured)))
# map => (movie_id, (genre, title, favoured))
temp_join = genres.join(movies). \
    map(lambda x: (x[0], (x[1][0], x[1][1][0], x[1][1][1])))

# join => (movie_id, (genre, title, favoured), (user_id, rating))
final_join = temp_join.join(ratings)

# Processing
# TODO

# Outputs
# for result in results.collect():
#     print(result)

# Print time spent for execution
print("---Completed in %s seconds ---" % (time.time() - start_time))