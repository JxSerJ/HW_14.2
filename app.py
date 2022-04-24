from flask import Flask, jsonify

from utils import DBHandler

application = Flask(__name__)

application.config.from_pyfile("config.py")

DB_Obj = DBHandler(application.config.get("DB_PATH"))


@application.route("/")
def navigation_page():
    return "INDEX PAGE"


@application.route("/movie/<title>")
def get_by_title(title: str):
    return_ = DB_Obj.get_db_data_by_title(title)
    if type(return_) == list:
        return jsonify(return_)
    return return_


@application.route("/movie/<int:year1>/to/<int:year2>")
def get_by_years(year1: int, year2: int):
    return_ = DB_Obj.get_db_data_by_years(year1, year2)
    if type(return_) == list:
        return jsonify(return_)
    return return_


@application.route("/rating/<rating>")
def get_by_rating(rating: str):
    return_ = DB_Obj.get_db_data_by_rating(rating)
    if type(return_) == list:
        return jsonify(return_)
    return return_


@application.route("/genre/<genre>")
def get_by_genre(genre: str):
    return_ = DB_Obj.get_db_data_by_genre(genre)
    if type(return_) == list:
        return jsonify(return_)
    return return_


@application.route("/movie/<actor1>/<actor2>")
def get_by_actors(actor1: str, actor2: str):
    return_ = DB_Obj.get_db_data_by_actor_names(actor1, actor2)
    return jsonify(return_)


@application.route("/movie/<entry_type>/<int:year>/<genre>")
def get_by_type_year_genre(entry_type: str, year: int, genre: str):
    return_ = DB_Obj.get_db_data_by_type_year_genre(entry_type, year, genre)
    if type(return_) == list:
        return jsonify(return_)
    return return_


if __name__ == "__main__":
    application.run(port=8001)
