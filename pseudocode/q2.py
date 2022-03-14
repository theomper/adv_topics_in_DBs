# ratings = ratings.csv

map(ratings, value):
    for movie in movies:
        line = movie.split(',')
        # ratings = (user_id, movie_id, rating, timestamp))
        user_id = line[0]
        rating = line[2]
        emit(user_id, (rating, 1))

reduce(user_id, (rating, 1)):
    sum_of_ratings = 0
    cnt_of_movies = 0
    for rating, cnt in [(rating, 1)]:
        sum_of_ratings += rating
        cnt_of_movies++
    emit(user_id, (sum_of_ratings, cnt_of_movies))


map(user_id, (sum_of_ratings, cnt_of_movies)):
    avg_rating = sum_of_ratings / cnt_of_movies
    emit(user_id, avg_rating)

## TODO Pythonwise or M-Rwise??
all_users = (user_id, avg_rating).count()
users_above_3 = (user_id, avg_rating).filter(avg_rating >= 3.0)

percentage = users_above_3 / all_users * 100