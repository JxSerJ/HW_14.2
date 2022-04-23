import sqlite3


class DBHandler:

    def __init__(self, db_path: str):
        pass

    def get_db_data_by_id(self, entry_id: str) -> dict:
        pass

    def get_db_data_by_years(self, start_year: int, end_year: int) -> list[dict]:
        pass

    def get_db_data_by_rating(self, rating: str) -> list[dict]:
        pass

    def get_db_data_by_genre(self, genre: str) -> list[dict]:
        pass

    def get_db_data_by_actor_names(self, actor_name1: str, actor_name2: str) -> list:
        pass

    def get_db_data_by_type_year_genre(self, type: str, release_year: int, genre: str) -> list[dict]:
        pass
