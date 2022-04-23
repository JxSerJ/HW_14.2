import sqlite3
from typing import Iterable


class DBHandler:

    def __init__(self, db_path: str):
        self.db_path = db_path
        print(f"SQL Data Base handler initialized with db: {db_path}")

    def db_connector(self, query: str, values: Iterable | None) -> list:
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
            return "Data not found"

        return data

    def get_db_data_by_years(self, start_year: int, end_year: int) -> list[dict]:

        query = ("SELECT `title`, `release_year` "
                 "FROM netflix "
                 "WHERE `release_year` BETWEEN '%s' AND '%s' "
                 "AND `type` = 'Movie' "
                 "LIMIT 100 ")

        values = (start_year, end_year)

        db_fetch_result = self.db_connector(query, values)

        data = []

        for row in db_fetch_result:
            data.append({
                "title": row[0],
                "release_year": row[1]
            })

        return data

    def get_db_data_by_rating(self, rating: str) -> list[dict] | str:

        query_primary = ("SELECT rating "
                         "FROM netflix "
                         "WHERE rating is not null "
                         "AND rating != '' "
                         "GROUP BY rating")

        allowed_ratings = []
        allowed_ratings_raw = self.db_connector(query_primary, None)

        for rating_str in allowed_ratings_raw:
            allowed_ratings.append(rating_str[0])

        try:
            if rating not in allowed_ratings:
                raise ValueError
        except ValueError:
            return "Rating not found in database"

        query = ("SELECT `title`, `rating`, `description` "
                 "FROM netflix "
                 "WHERE `rating` == '%s' "
                 "AND `type` = 'Movie' "
                 "LIMIT 100 ")

        values = rating

        db_fetch_result = self.db_connector(query, values)

        data = []

        for row in db_fetch_result:
            data.append({
                "title": row[0],
                "rating": row[1],
                "description": row[2]
            })

        return data

    def get_db_data_by_genre(self, genre: str) -> list[dict]:
        pass

    def get_db_data_by_actor_names(self, actor_name1: str, actor_name2: str) -> list:
        pass

    def get_db_data_by_type_year_genre(self, entry_type: str, release_year: int, genre: str) -> list[dict]:
        pass
