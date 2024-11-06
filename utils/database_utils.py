import psycopg2 as pg
from params import database_params as dbc
from sqlalchemy import create_engine


def create_connection_database():
    url = (
        "postgresql+psycopg2://postgres:"
        + dbc.PASSWORD
        + "@localhost:"
        + dbc.PORT
        + "/"
        + dbc.DATABASE
    )
    return create_engine(url)

