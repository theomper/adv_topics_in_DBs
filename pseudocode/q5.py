# movies = movies.csv
# genres = genres.csv
# ratings = ratings.csv

map(movies, value):
    for movie in movies:
        line = movie.split(',')
        # movie = (movie_id, title, description, publish_date,
        #          duration, cost, income, favoured)
        movie_id = line[0]
        title = line[1]
        favoured = line[7]
        emit(movie_id, (title, favoured))

map(genres, value):
    for genre in genres:
        line = genre.split(',')
        # genres = (movie_id, genre)
        movie_id = line[0]
        genre = line[1]
        emit(movie_id, genre)

map(ratings, value):
    for movie in movies:
        line = movie.split(',')
        # ratings = (user_id, movie_id, rating, timestamp))
        user_id = line[0]
        movie_id = line[1]
        rating = line[2]
        emit(movie_id, (user_id, rating))

join((movie_id, genre), (movie_id, (title, favoured)):
    emit(movie_id, (genre, (title, favoured)))

map(movie_id, (genre, (title, favoured))):
    emit(movie_id, (genre, title, favoured))

join((movie_id, (genre, title, favoured)), (movie_id, (user_id, rating))):
    emit(movie_id, ((genre, title, favoured), (user_id, rating)))

map(movie_id, ((genre, title, favoured), (user_id, rating))):
    max_title = min_title = title
    max_rating = min_rating = rating
    max_favoured = min_favoured = favoured
    emit((user_id, genre), (1, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured))

reduce((user_id, genre), (1, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured)):
    count = 0
    max_rating = max_favoured = 0
    min_rating = min_favoured = large_number
    for datum in data:
        count++
        if ((datum[2] > max_rating) or ((datum[2] == max_rating) and (datum[3] > max_favoured))):
            max_title = datum[1]
            max_rating = datum[2]
            max_favoured = datum[3]
        if ((datum[5] < min_rating) or ((datum[5] == min_rating) and (datum[6] > min_favoured))):
            min_title = datum[4]
            min_rating = datum[5]
            min_favoured = datum[6]
    emit((genre, userID), (count, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured))

map((user_id, genre), (count, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured)):
    emit(genre, (user_id, count, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured))

reduce(genre, (user_id, count, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured)):
    max_count = 0
    for line in lines:
        if (count > max_count):
            max_count = count
            assign max accordingly to every attribute
    emit(genre, (user_id, max_count, max_title, max_rating, min_title, min_rating))

map(genre, (user_id, max_count, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured)):
    emit(genre, user_id, max_count, max_title, max_rating, max_favoured, min_title, min_rating, min_favoured))