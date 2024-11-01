import notebook.crawling as crawling
from utils import database_utils as db
import pandas as pd
from params import tables_params as tp


# DIM_DATE table
def insert_data_into_dim_date_table(data):
    date_df = split_date_to_week_month_quater_year(data)

    connection = db.create_connection_database()
    date_df.to_sql(name=tp.DIM_DATE_TABLE, con=connection, if_exists="append", index=False)
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.DIM_DATE_TABLE} "
            f"ADD PRIMARY KEY ({tp.DATE_COLUMN});"
        )


def split_date_to_week_month_quater_year(data):
    date_df = data[[tp.DATE_COLUMN]]
    date_df.drop_duplicates(inplace=True)
    date_df.date = pd.to_datetime(date_df.date)

    date_df[tp.WEEK_COLUMN] = (
        date_df[tp.DATE_COLUMN].dt.isocalendar().week.apply(lambda x: "Week_" + str(x))
    )
    date_df[tp.MONTH_COLUMN] = date_df[tp.DATE_COLUMN].apply(lambda x: str(x)[:7])
    date_df[tp.QUARTER_COLUMN] = date_df[tp.DATE_COLUMN].dt.quarter.apply(lambda x: "Q" + str(x))
    date_df[tp.YEAR_COLUMN] = date_df[tp.DATE_COLUMN].dt.year.astype("str")

    return date_df


# DIM_SYMBOL table
def insert_data_into_dim_symbol_table(data):
    symbol_df = data[[tp.SYMBOL_COLUMN]]
    symbol_df.drop_duplicates(inplace=True)

    connection = db.create_connection_database()
    symbol_df.to_sql(name=tp.DIM_SYMBOL_TABLE, con=connection, if_exists="append", index=False)
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.DIM_SYMBOL_TABLE} "
            f"ADD PRIMARY KEY ({tp.SYMBOL_COLUMN});")


# DIM_FACT_GOLD_PRICE table
def insert_data_in_current_year_into_fact_gold_data_table(data):
    connection = db.create_connection_database()
    data.to_sql(name=tp.FACT_GOLD_DATA_TABLE, con=connection, if_exists="append", index=False)
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.FACT_GOLD_DATA_TABLE} "
            f"ADD FOREIGN KEY ({tp.DATE_COLUMN}) REFERENCES dim_date({tp.DATE_COLUMN}), "
            f"ADD FOREIGN KEY ({tp.SYMBOL_COLUMN}) REFERENCES dim_symbol({tp.SYMBOL_COLUMN});"
        )


# insert data into DATABASE
def insert_data_into_database_from_beginning():
    data = crawling.crawl_data_in_current_year()

    insert_data_into_dim_symbol_table(data)
    insert_data_into_dim_date_table(data)
    insert_data_in_current_year_into_fact_gold_data_table(data)


def insert_data_in_database_everyday():
    data = crawling.crawl_data_every_day()

    insert_data_into_dim_symbol_table(data)
    insert_data_into_dim_date_table(data)
    insert_data_in_current_year_into_fact_gold_data_table(data)


# CALL insert functions
insert_data_into_database_from_beginning()
