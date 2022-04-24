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
            return "Data not found in database"

        return data

    def get_db_data_by_years(self, start_year: int, end_year: int) -> list[dict] | str:

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

        if len(data) == 0:
            return "Data not found in database"

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
            return "Rating or rating group not found in database. Or you just tried SQL-injection. This is not good."

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

        query = ("SELECT `title`, `date_added`, `listed_in`, `description`, `release_year` "
                 "FROM netflix "
                 "WHERE `listed_in` IS NOT '' "
                 "AND `listed_in` IS NOT NULL "
                 "AND `listed_in` LIKE '%s' "
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
            return "Data not found in database"

        return data

    def get_db_data_by_actor_names(self, actor_name1: str = None, actor_name2: str = None) -> list | str:

        query = ("SELECT `cast`, `title` "
                 "FROM netflix "
                 "WHERE `cast` IS NOT '' "
                 "AND `cast` IS NOT NULL "
                 "LIMIT 5 ")

        values = None

        db_fetch_result = self.db_connector(query, values)

        data = []

        print(db_fetch_result)

        for row in db_fetch_result:
            data.append({
                "cast": row[0],
                "title": row[1]
            })

        if len(data) == 0:
            return "Data not found in database"

        return data

    def get_db_data_by_type_year_genre(self, entry_type: str, release_year: int, genre: str) -> list[dict] | str:

        query = ("SELECT `title`, `type`, `listed_in`, `release_year`, `description` "
                 "FROM netflix "
                 "WHERE `listed_in` IS NOT '' "
                 "AND `listed_in` IS NOT NULL "
                 "AND `listed_in` LIKE '%s' "
                 "AND `type` LIKE '%s' "
                 "AND `release_year` = '%s' "
                 "ORDER BY `release_year` DESC, date_added DESC "
                 "LIMIT 500 ")

        values = ('%' + genre + '%', entry_type, release_year)

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
            return "Data not found in database"

        return data
