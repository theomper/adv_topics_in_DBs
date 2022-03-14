# movies = movies.csv
# genres = genres.csv

map(movies, value):
    for movie in movies:
        line = movie.split(',')
        # movie = (movie_id, title, description, publish_date,
        #          duration, cost, income, favoured)
        movie_id = line[0]
        title = line[1]
        description = line[2]
        publish_date = line[3]
        year = line[3].split('-')[0]
        if ((title != '') and (publish_date != '') and
            (year != '') and (year >= 2000) and (year <= 2019)):
            word_count_in_description = 0
            for word in description.split():
                word_count_in_description++
            emit(movie_id, (year, word_count_in_description))

map(genres, value):
    for genre in genres:
        line = genre.split(',')
        # genres = (movie_id, genre)
        movie_id = line[0]
        genre = line[1]
        if (genre = ' Drama'):
            emit(movie_id, 'Drama')

join((movie_id, (year, word_count_in_description)), (movie_id, 'Drama')):
    emit(movie_id, ((year, word_count_in_description), 'Drama'))

map(movie_id, ((year, word_count_in_description), 'Drama')):
    if (year >=2000) and (year <=2004):
        period = '2000-2004'
    elif (year >=2005) and (year <=2009):
        period = '2005-2009'
    elif (year >=2010) and (year <=2014):
        period = '2010-2014'
    else:
        period = '2015-2019'
    emit(period, (word_count_in_description, 1))

reduce(period, (word_count_in_description, 1)):
    sum_of_words = 0
    cnt_of_movies = 0
    for words, cnt in (word_count_in_description, 1):
        sum_of_words += words
        cnt_of_movies++
    emit(period, (sum_of_words, cnt_of_movies))

map(period, (sum_of_words, cnt_of_movies)):
    avg_cnt_of_words = sum_of_words / cnt_of_movies
    emit(period, avg_cnt_of_words)