# code for capturing execution time copied from
# https://stackoverflow.com/questions/1557571/how-do-i-get-time-of-a-python-programs-execution
# code for max value through reduceByKey from
# https://stackoverflow.com/questions/52137351/pyspark-python-reducebykey-filter-by-math-max

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

def calc_profit(cost, income):
    return ((income-cost)/cost)*100

# Start clock
start_time = time.time()

spark = SparkSession.builder.appName("q1_rdd").getOrCreate()
sc = spark.sparkContext

# Input - Movies
# map => (movie_id, title, description, publish_date, duration, cost, income, favoured)
# filter out ((publish_date == '') && (cost == 0) && (income == 0) && (year > 2000))
# map => (year, (title, profit))
# reduce => (year, (title, max(profit)))
# result[0] = year
# result[1][0] = title
# result[1][1] = profit
results = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map(lambda x: split_complex(x)). \
    filter(lambda x: x[3] != ''
        and (int(x[5]) != 0)
        and (int(x[6]) != 0)
        and (int(extract_year(x[3])) >= 2000)). \
    map(lambda x: (extract_year(x[3]), (x[1], calc_profit(int(x[5]), int(x[6]))))). \
    reduceByKey(lambda x,y: max((x, y), key=lambda x: x[1])). \
    sortByKey()

# Output
# result = (year, (title, profit)) => (year, title)
for result in results.collect():
    print ("Year = ", result[0], "Title = ", result[1][0])

# Print time spent for execution
print("---Completed in %s seconds ---" % (time.time() - start_time))