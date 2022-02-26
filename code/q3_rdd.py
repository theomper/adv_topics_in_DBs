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

spark = SparkSession.builder.appName("q3_rdd").getOrCreate()

sc = spark.sparkContext

# convert ratings => (movieId, rating)
ratings = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x: split_complex(x)).\
    map(lambda x: (int(x[1]), float(x[2])))

# (movieId, genre)
movie_genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x: split_complex(x)).\
    map(lambda x: (int(x[0]), x[1]))

# results [movieId, (rating, genre)]
results = ratings.join(movie_genres)

# possible bug in the reduceByKey -> drops useful data
avg_rtg_movie = results. \
    map(lambda x: (x[0], (x[1][0], x[1][1], 1))). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1], x[2] + y[2])). \
    map(lambda x: (x[0], (x[1][1], x[1][0]/x[1][2])))

#  or possible bug in the reduceByKey -> drops useful data
avg_rtg_genre = avg_rtg_movie. \
    map(lambda x: (x[1][0], (x[1][1], 1))). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], x[1][0]/x[1][1], x[1][1]))

# Print results
for result in avg_rtg_genre.collect():
    print(result)

# Print time spent for execution
print("--- %s seconds ---" % (time.time() - start_time))