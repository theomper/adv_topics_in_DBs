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

spark = SparkSession.builder.appName("q2_rdd").getOrCreate()

sc = spark.sparkContext

# x[0] = userId
# x[1] = movieId
# x[2] = rating
# first map => (userId, (rating, 1))
# reduceByKey (userId, (sumOfRatings, countOfMovies))
# second map => (userId, sumOfRatings / countOfMovies)
results = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x: split_complex(x)). \
    map(lambda x: (int(x[0]), (float(x[2]), 1))). \
    reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], x[1][0]/x[1][1]))

# count all_users
all_users = results.count()

# count users3 with avg rating > 3.0
users_above_3 = results.filter(lambda x: x[1] > 3.0).count()

percentage = users_above_3 / all_users * 100

# Print results
print(str(percentage) + "%")

# Print time spent for execution
print("--- %s seconds ---" % (time.time() - start_time))