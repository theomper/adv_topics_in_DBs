# ratings = ratings.csv
# genres = genres.csv

map(ratings, value):
    for movie in movies:
        line = movie.split(',')
        # ratings = (user_id, movie_id, rating, timestamp))
        movie_id = line[1]
        rating = line[2]
        emit(movie_id, (rating, 1))

reduce(movie_id, (rating, 1)):
    sum_of_ratings = 0
    cnt_of_ratings = 0
    for rating, cnt in (rating, 1):
        sum_of_ratings += rating
        cnt_of_ratings++
    emit(movie_id, (sum_of_ratings, cnt_of_ratings))

map(movie_id, (sum_of_ratings, cnt_of_ratings)):
    avg_rating = sum_of_ratings / cnt_of_ratings
    emit(movie_id, (avg_rating, cnt_of_ratings))


map(genres, value):
    for genre in genres:
        line = genre.split(',')
        # genres = (movie_id, genre)
        movie_id = line[0]
        genre = line[1]
        emit(movie_id, genre)

join((movie_id, genre), (movie_id, (avg_rating, cnt_of_ratings))):
    emit(movie_id, (genre, (avg_rating, cnt_of_ratings)))

map(movie_id, (genre, (avg_rating, cnt_of_ratings))):
    for line in lines:
        emit(genre, (avg_rating, 1))

reduce(genre, (avg_rating, 1)):
    sum_of_avg_ratings = 0
    cnt_of_avg_ratings = 0
    for avg_rating, cnt in (avg_rating, 1):
        sum_of_avg_ratings += avg_rating
        cnt_of_avg_ratings++
    emit(movie_id, (sum_of_avg_ratings, cnt_of_avg_ratings))

map(movie_id, (sum_of_avg_ratings, cnt_of_avg_ratings)):
    avg_rating_per_genre = sum_of_avg_ratings / cnt_of_avg_ratings
    emit(genre, (avg_rating_per_genre, cnt_of_avg_rating_per_genre))

map(genre, (avg_rating_per_genre, cnt_of_avg_rating_per_genre))
    emit(genre, avg_rating_per_genre, cnt_of_avg_rating_per_genre)
