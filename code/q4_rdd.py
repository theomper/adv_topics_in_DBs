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

# Count words in string
# Use of split() without argument to catch all whispaces (space, \n, \t)
def count_words(x):
    res = len(x.split())
    return res

# Split into 5year chunks by renaming year column
def split_to_5years(x):
    if (x >=2000) and (x <=2004):
        return '2000-2004'
    elif (x >=2005) and (x <=2009):
        return '2005-2009'
    elif (x >=2010) and (x <=2014):
        return '2010-2014'
    else:
        return '2015-2019'

# Start clock
start_time = time.time()

spark = SparkSession.builder.appName("q4_rdd").getOrCreate()

sc = spark.sparkContext

# x[2] = description
# x[3] = publish date
# movies = (movieId, (year, word_count_in_desc))
# movies[0] = movieId
# movies[1][0] = year
# movies[1][1] = word_count_in_desc
movies = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map(lambda x: split_complex(x)). \
    filter(lambda x: 
        (x[2] != '')
        and (x[3] != '')
        and (extract_year(x[3]) != '')
        and (int(extract_year(x[3])) >= 2000)
        and (int(extract_year(x[3])) <= 2019)). \
    map(lambda x: (int(x[0]), (int(extract_year(x[3])), count_words(x[2]))))

# (movieId, 'Drama')
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x: split_complex(x)).\
    filter(lambda x: x[1] == 'Drama'). \
    map(lambda x: (int(x[0]), x[1]))

# results after joining movies + genres {movieId, [(year, word_count_in_desc), Drama]}
joined = movies.join(genres)

# map ['20xx-20xx', (word_count_in_desc, 1)]
# reduce ['20xx-20xx', sum_of_words, count_of_movies]
# map ['20xx-20xx', avg_count_of_words]
results = joined. \
    map(lambda x: (split_to_5years(x[1][0][0]), (x[1][0][1], 1))).\
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], x[1][0] / x[1][1])). \
    sortByKey()

# Print results
for result in results.collect():
    print(result)

# Print time spent for execution
print("--- %s seconds ---" % (time.time() - start_time))