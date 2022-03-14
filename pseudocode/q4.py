# movies = movies.csv
# genres = genres.csv

#TODO fixation of these maps a the beginning

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
            (year != '') and (year > 2000) and (year < 2019):
            ##TODO find word count
            emit(movie_id, (year, word_count_in_description))

map(genres, value):
    for genre in genres:
        line = genre.split(',')
        # genres = (movie_id, genre)
        movie_id = line[0]
        genre = line[1]
        if (genre = ' Drama')
            emit(movie_id, 'Drama')

join([(...)], [(movie_id, 'Drama')])
    emit(movie_id, (, ()))

map():
##TODO

reduce():
##TODO

map():
##TODO