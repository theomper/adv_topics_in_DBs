# movies = movies.csv

map(movies, value):
    for movie in movies:
        line = movie.split(',')
        # movie = (movie_id, title, description, publish_date,
        #          duration, cost, income, favoured)
        title = line[1]
        year = line[3].split('-')[0]
        publish_date = line[3]
        cost = line[5]
        income = line[6]
        if ((publish_date != '') and (cost != 0) and
            (income != 0) and (year > 2000)):
            profit = ((income - cost) / cost) * 100
            emit(year, (title, profit))

reduce(year, (title, profit)):
    max_profit = 0
    max_title = ''
    for title, profit in (title, profit):
        if (max_profit > profit):
            max_profit = profit
            max_title = title
    emit(year, (max_title, max_profit))

map(year, (max_title, max_profit)):
    emit(year, title)