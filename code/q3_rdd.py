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

spark = SparkSession.builder.appName("q3_rdd").getOrCreate()

sc = spark.sparkContext

# map => (user_id, movie_id, rating, timestamp)
# map => (movieId, (rating, 1))
# reduce => (movieId, (sum_of_ratings, cnt_of_ratings))
# map => (movieId, (avg_rating, cnt_of_ratings))
ratings = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x: split_complex(x)). \
    map(lambda x: (int(x[1]), (float(x[2]), 1))). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], (x[1][0] / x[1][1], x[1][1])))

# map => (movieId, genre)
movie_genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x: split_complex(x)).\
    map(lambda x: (int(x[0]), x[1]))

# join => (movieId, (genre, (avg_rating, cnt_of_ratings)))
joined = movie_genres.join(ratings)

# map => (genre, (avg_rating, 1))
# reduce => (genre, (sum_of_avg_ratings, cnt_of_avg_ratings))
# map => (genre, (avg_rating_per_genre, cnt_of_avg_rating_per_genre))
results = joined. \
    map(lambda x: (x[1][0], (x[1][1][0], 1))). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1])))

# Print results
for result in results.collect():
    print(result)

# Print time spent for execution
print("--- %s seconds ---" % (time.time() - start_time))