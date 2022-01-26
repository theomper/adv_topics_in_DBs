# Structure
```
path
└── /03111612_03100001_03100002/
    ├── code/
    |
    ├── outputs/
    |
    ├── ip.md
    |
    └── report.pdf
```

# Μέρος 1o
## Ζητούμενο 1
```shell
wget 'http://www.cslab.ntua.gr/courses/atds/movie_data.tar.gz'

tar -zxf movie_data.tar.gz

hadoop fs -mkdir hdfs://master:9000/files

hadoop fs -put movies.csv hdfs://master:9000/files/.
hadoop fs -put movie_genres.csv hdfs://master:9000/files/.
hadoop fs -put ratings.csv hdfs://master:9000/files/.

hadoop fs -mkdir hdfs://master:9000/outputs
```
<!-- delimiter char for .csv = ',' -->

movies.csv // 45K entries // 17MB
(movie_id, title, description, publish_date, duration, cost, income, favoured)
```
Indices
movie_id    -> 0
title       -> 1
description -> 2
publish_date-> 3
duration    -> 4
cost        -> 5
income      -> 6
favoured    -> 7
```
________________
8844,Jumanji,When siblings Judy and Peter discover an enchanted board game that opens the door to a magical world they unwittingly invite Alan an adult whos been trapped inside the game for 26 years into their living room Alans only hope for freedom is to finish the game which proves risky as all three find themselves running from giant rhinoceroses evil monkeys and other terrifying creatures,1995-12-15T00:00:00.000+02:00,104.0,65000000,262797249,17.015539
_______________

movie_genres.csv // 91K entries // 1.3MB
(movie_id, genre)
```
Indices
movie_id    -> 0
genre       -> 1
```
| movie_id  | Genre     |
| --------- | --------- |
| 862       | Animation |
| 862       | Comedy    |
| 862       | Family    |
| 8844      | Adventure |
| 8844      | Fantasy   |
| 8844      | Family    |

ratings.csv // 26M entries ! // 677MB !
(user_id, movie_id, rating, timestamp)
```
Indices
user_id     -> 0
movie_id    -> 1
rating      -> 2
timestamp   -> 3
```

| user_id   | movie_id  | rating    | timestamp |
| --------- | --------- | --------- | --------- |
| 1         | 110       | 1.0       | 1425941529|
| 1         | 147       | 4.5       | 1425942435|
| 1         | 858       | 5.0       | 1425941523|
| 1         | 1221      | 5.0       | 1425941546|

## Ζητούμενο 2
.csv to Parquet

## Ζητούμενο 3

________________________________________________________________________________
### Q1 (movies.csv)
```
for each year 
where publish_date(year) >= 2000
print movie with max(profit= ((income-cost)/cost)*100

exclude movies with (publish_date == null) || (income == null) || (cost == null)
```
________________________________________________________________________________
### Q2 (ratings.csv)
```
find percentage of users with (avg rating > 3.0)
```
________________________________________________________________________________
### Q3 (movies.csv + movies_genres.csv + ratings.csv)


________________________________________________________________________________
### Q4 ()
```
exclude movies with (description == null)
```
________________________________________________________________________________
### Q5 ()


________________________________________________________________________________

## Ζητούμενο 4
One script to exec them all. One script to run them. One script to print them all and in the /outputs print them.

Hint 1
```
spark-submit q1_rdd.py > ouput
```
Writes ONLY every print() on the 'output' file

Hint 2
```
spark-submit q1_rdd.py > logs 2>&1
```
Writes everything (logs + prints()) on the 'logs' file

# Μέρος 2o
## Ζητούμενο 1

## Ζητούμενο 2

## Ζητούμενο 3

## Ζητούμενο 4

