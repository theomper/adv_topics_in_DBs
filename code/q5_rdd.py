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

# movies
movies = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map(lambda x: split_complex(x))

# genres
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x: split_complex(x))

# ratings
ratings = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x: split_complex(x))

# Print results
# for result in results.collect():
#     print(result)

# Print time spent for execution
print("--- %s seconds ---" % (time.time() - start_time))