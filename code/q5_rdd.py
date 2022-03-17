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

spark = SparkSession.builder.appName("q5_rdd").getOrCreate()
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
    map(lambda x: (x.split(',')[0], x.split(',')[1])).distinct()

# map => (user_id, movie_id, rating, timestamp)
# map => (movie_id, (user_id, rating))
ratings = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x: (x.split(',')[1], (x.split(',')[0], float(x.split(',')[2]))))

# Joins
# join => (movie_id, (genre, (title, favoured)))
# map => (movie_id, (genre, title, favoured))
temp_join = genres.join(movies). \
    map(lambda x: (x[0], (x[1][0], x[1][1][0], x[1][1][1])))

# Processing
# join => (movie_id, ((genre, title, favoured), (user_id, rating)))
# map => ((user_id, genre), (1, title, rating, favoured, title, rating, favoured))
# reduce => ((user_id, genre), (count, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured))
# map => (genre, (user_id, count, max_title, max_rating, min_title, min_rating))
# reduce => (genre, (user_id, max_count, max_title, max_rating, min_title, min_rating))
# map => (genre, user_id, count, max_title, max_rating, min_title, min_rating))
final_join = temp_join.join(ratings).map(lambda x: ((x[1][0][0], x[1][1][0]), (1, x[1][0][1], x[1][1][1], x[1][0][2], x[1][0][1], x[1][1][1], x[1][0][2]))). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1] if x[2]>y[2] else (y[1] if y[2]>x[2] else (x[1] if x[3]>y[3] else y[1])),
                                        x[2] if x[2]>y[2] else (y[2] if y[2]>x[2] else (x[2] if x[3]>y[3] else y[2])),
                                        x[3] if x[2]>y[2] else (y[3] if y[2]>x[2] else (x[3] if x[3]>y[3] else y[3])),
                                        x[4] if x[5]<y[5] else (y[4] if y[5]<x[5] else (x[4] if x[6]>y[6] else y[4])),
                                        x[5] if x[5]<y[5] else (y[5] if y[5]<x[5] else (x[5] if x[6]>y[6] else y[5])),
                                        x[6] if x[5]<y[5] else (y[6] if y[5]<x[5] else (x[6] if x[6]>y[6] else y[6])))). \
    map(lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1], x[1][2], x[1][4], x[1][5]))). \
    reduceByKey(lambda x, y: (x[0], x[1], x[2], x[3], x[4], x[5]) if x[1]>y[1] else (y[0], y[1], y[2], y[3], y[4], y[5])). \
    sortByKey(ascending=True). \
    map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))

# Outputs
for result in final_join.collect():
    print(result)

# Print time spent for execution
print("---Completed in %s seconds ---" % (time.time() - start_time))