from flask import Flask

from utils import DBHandler

application = Flask(__name__)

application.config.from_pyfile("config.py")

DB_Obj = DBHandler(application.config.get("DB_PATH"))


@application.route("/")
def navigation_page():
    pass


@application.route("/movie/<title>")
def get_by_title(title: str):
    pass


@application.route("/movie/<int:year1>/to/<int:year2>")
def get_by_years(year1: int, year2: int):
    pass


@application.route("/rating/<rating>")
def get_by_rating(rating: str):
    pass


@application.route("/genre/<genre>")
def get_by_genre(genre: str):
    pass


@application.route("/movie/<actor1>/<actor2>")
def get_by_actors(actor1: str, actor2: str):
    pass


@application.route("/movie/<type>/<int:year>/<genre>")
def get_by_type_year_genre(type: str, year: int, genre: str):
    pass


if __name__ == "__main__":
    application.run()
