# code for capturing execution time copied from
# https://stackoverflow.com/questions/1557571/how-do-i-get-time-of-a-python-programs-execution

from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

# Given help function 
def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

# Extract year from timestamp '1995-10-30T00:00:00.000+02:00'
def extract_year(x):
    return x.split("-")[0]

# using split() to count words in string
def count_words(x):
    res = len(x.split())
    return res

# Start clock
start_time = time.time()

spark = SparkSession.builder.appName("q4_rdd").getOrCreate()

sc = spark.sparkContext

# (movieId, word_count_in_desc)
movies = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map(lambda x: split_complex(x)). \
    filter(lambda x: x[2] != ''
        and x[3] != ''
        and (int(extract_year(x[3])) >= 2000)
        and (int(extract_year(x[3])) <= 2019)). \
    map(lambda x: (int(x[0]), count_words(str(x[2]))))

# (movieId, genre)
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x: split_complex(x)).\
    filter(lambda x: x[1] == 'Drama'). \
    map(lambda x: (int(x[0]), x[1]))

# results [movieId, (word_count_in_desc, Drama)]
results = movies.join(genres)

# Print results
for result in results.collect():
    print(result)

# Print time spent for execution
print("--- %s seconds ---" % (time.time() - start_time))