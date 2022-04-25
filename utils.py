import sqlite3
from typing import Iterable, Any


class DBHandler:

    def __init__(self, db_path: str):
        self.db_path = db_path
        print(f"SQL Data Base handler initialized with db: {db_path}")

    def db_connector(self, query: str, values: Iterable[Any] | None = None) -> list:
        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.cursor()
            if values is None:
                return cursor.execute(query).fetchall()
            return cursor.execute(query % values).fetchall()

    def get_db_data_by_title(self, title: str) -> dict | str:

        query = ("SELECT `title`, `country`, `release_year`, `listed_in`, `description` "
                 "FROM netflix "
                 "WHERE `title` LIKE '%s' "
                 "AND `type` = 'Movie' "
                 "ORDER BY `release_year` DESC, date_added DESC "
                 "LIMIT 1 ")

        values = '%' + title + '%'

        db_fetch_result = self.db_connector(query, values)

        try:
            data = {
                "title": db_fetch_result[0][0],
                "country": db_fetch_result[0][1],
                "release_year": db_fetch_result[0][2],
                "genre": db_fetch_result[0][3],
                "description": db_fetch_result[0][4]
            }
        except IndexError:
            return "<H1 style='font-family: monospace'>Data not found in database</H1>"

        return data

    def get_db_data_by_years(self, start_year: int, end_year: int) -> list[dict] | str:

        if start_year > end_year:
            return "<H1 style='font-family: monospace'>Года наоборот нужно вводить!!1!</H1>"

        query = ("SELECT `title`, `release_year` "
                 "FROM netflix "
                 "WHERE `release_year` BETWEEN '%s' AND '%s' "
                 "AND `type` = 'Movie' "
                 "ORDER BY release_year ASC "
                 "LIMIT 100 ")

        values = (start_year, end_year)

        db_fetch_result = self.db_connector(query, values)

        data = []

        for row in db_fetch_result:
            data.append({
                "title": row[0],
                "release_year": row[1]
            })

        if len(data) == 0:
            return "<H1 style='font-family: monospace'>Data not found in database</H1>"

        return data

    def get_db_data_by_rating(self, rating: str) -> list[dict] | str:

        query_primary = ("SELECT rating "
                         "FROM netflix "
                         "WHERE rating is not null "
                         "AND rating != '' "
                         "GROUP BY rating")

        allowed_ratings = []
        allowed_ratings_raw = self.db_connector(query_primary, None)

        allowed_user_groups = {'children': ['G'], 'family': ['G', 'PG', 'PG-13'], 'adult': ['R', 'NC-17']}

        for rating_str in allowed_ratings_raw:
            allowed_ratings.append(rating_str[0])
        try:
            if rating in allowed_ratings:
                query = ("SELECT `title`, `rating`, `description` "
                         "FROM netflix "
                         "WHERE `rating` == '%s' "
                         "AND `type` = 'Movie' "
                         "LIMIT 100 ")
                values = rating

            elif rating in allowed_user_groups.keys():
                rating_list = "'" + "', '".join(allowed_user_groups[rating]) + "'"
                query = ("SELECT `title`, `rating`, `description` "
                         "FROM netflix "
                         f"WHERE `rating` IN ({rating_list}) "  # TODO idk how to implemet it without f-string (%s and ? doesn't work)
                         "AND `type` = 'Movie' "
                         "LIMIT 100 ")
                values = None
            else:
                raise ValueError
        except ValueError:
            return "<H1 style='font-family: monospace'>Rating or rating group not found in database. Or you just tried SQL-injection. This is not good.</H1>"

        db_fetch_result = self.db_connector(query, values)

        data = []

        for row in db_fetch_result:
            data.append({
                "title": row[0],
                "rating": row[1],
                "description": row[2]
            })

        return data

    def get_db_data_by_genre(self, genre: str) -> list[dict] | str:

        query = ("SELECT `title`, `date_added`, `listed_in` AS genre, `description`, `release_year` "
                 "FROM netflix "
                 "WHERE `genre` IS NOT '' "
                 "AND `genre` IS NOT NULL "
                 "AND `genre` LIKE '%s' "
                 "AND `type` = 'Movie' "
                 "ORDER BY `release_year` DESC, date_added DESC "
                 "LIMIT 10 ")

        values = '%' + genre + '%'

        db_fetch_result = self.db_connector(query, values)

        data = []

        for row in db_fetch_result:
            data.append({
                "title": row[0],
                "release_year": row[4],
                "genre": row[2],
                "description": row[3]
            })

        if len(data) == 0:
            return "<H1 style='font-family: monospace'>Data not found in database</H1>"

        return data

    def get_db_data_by_actor_names(self, actor_name1: str = None, actor_name2: str = None) -> list | str:

        query = ("SELECT `cast`, `title` "
                 "FROM netflix "
                 "WHERE `cast` IS NOT '' "
                 "AND `cast` IS NOT NULL "
                 "AND `cast` LIKE '%s' "
                 "AND `cast` LIKE '%s' "
                 "LIMIT 100 ")

        values = ('%' + actor_name1 + '%', '%' + actor_name2 + '%')

        db_fetch_result = self.db_connector(query, values)

        data = []
        actors = []

        for row in db_fetch_result:
            row_list = row[0].split(', ')

            for row_list_entry in row_list:
                if row_list_entry != actor_name1 and row_list_entry != actor_name2:
                    data.append(row_list_entry)

        for actor in data:
            if data.count(actor) > 2:
                actors.append(actor)

        if len(actors) == 0:
            return "<H1 style='font-family: monospace'>Data not found in database</H1>"

        actors = list(set(actors))

        return actors

    def get_db_data_by_type_year_genre(self, entry_type: str, release_year: int, genre: str) -> list[dict] | str:

        query = ("SELECT `title`, `type`, `listed_in` AS genre, `release_year`, `description` "
                 "FROM netflix "
                 "WHERE `genre` IS NOT '' "
                 "AND `genre` IS NOT NULL "
                 "AND `genre` LIKE '%s' "
                 "AND `type` LIKE '%s' "
                 "AND `release_year` = '%s' "
                 "ORDER BY `release_year` DESC, date_added DESC "
                 "LIMIT 500 ")

        values = ('%' + genre + '%', '%' + entry_type + '%', release_year)

        db_fetch_result = self.db_connector(query, values)

        data = []

        for row in db_fetch_result:
            data.append({
                "title": row[0],
                "type": row[1],
                "release_year": row[3],
                "genre": row[2],
                "description": row[4]
            })

        if len(data) == 0:
            return "<H1 style='font-family: monospace'>Data not found in database</H1>"

        return data
