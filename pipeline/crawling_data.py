import utils.crawling_utils as crawling
from utils import database_utils as db
import pandas as pd
from params import tables_params as tp
from sqlalchemy.util import deprecations
from datetime import timedelta

deprecations.SILENCE_UBER_WARNING = True


# DIM_DATE table
def insert_data_into_dim_date_table(data):
    date_df = split_date_to_week_month_quater_year(data)

    connection = db.create_connection_database()
    date_df.to_sql(
        name=tp.DIM_DATE_TABLE_NAME, con=connection, if_exists="append", index=False
    )
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.DIM_DATE_TABLE_NAME} "
            f"ADD PRIMARY KEY ({tp.DATE_COLUMN_NAME});"
        )


def insert_data_into_dim_date_table_everyday(data):
    date_df = split_date_to_week_month_quater_year(data)

    connection = db.create_connection_database()
    try:
        date_df.to_sql(
            name=tp.DIM_DATE_TABLE_NAME, con=connection, if_exists="append", index=False
        )
    except:
        print("Duplicate data")


def split_date_to_week_month_quater_year(data):
    date_col = data[[tp.DATE_COLUMN_NAME]]
    date_df = date_col.drop_duplicates(
        subset=tp.DATE_COLUMN_NAME, inplace=False
    ).reset_index(drop=True)

    date_df.insert(
        1,
        column=tp.WEEK_COLUMN_NAME,
        value=date_df[tp.DATE_COLUMN_NAME]
        .dt.isocalendar()
        .week.apply(lambda x: "Week_" + str(x)),
        allow_duplicates=False,
    )

    date_df.insert(
        2,
        column=tp.MONTH_COLUMN_NAME,
        value=date_df[tp.DATE_COLUMN_NAME].apply(lambda x: str(x)[:7]),
        allow_duplicates=False,
    )

    date_df.insert(
        3,
        column=tp.QUARTER_COLUMN_NAME,
        value=date_df[tp.DATE_COLUMN_NAME].dt.quarter.apply(lambda x: "Q" + str(x)),
        allow_duplicates=False,
    )

    date_df.insert(
        4,
        column=tp.YEAR_COLUMN_NAME,
        value=date_df[tp.DATE_COLUMN_NAME].dt.year.astype("str"),
        allow_duplicates=False,
    )

    return date_df


def insert_data_into_dim_date_table_everyday(data):
    date_df = split_date_to_week_month_quater_year(data)

    connection = db.create_connection_database()
    date_df.to_sql(
        name=tp.DIM_DATE_TABLE_NAME, con=connection, if_exists="append", index=False
    )


# REAL_TIME_DIM_DATE table
def insert_data_into_real_time_dim_date_table(data):
    date_df = split_date_to_hour_minute_second(data)

    connection = db.create_connection_database()
    date_df.to_sql(
        name=tp.REAL_TIME_DIM_DATE_TABLE_NAME,
        con=connection,
        if_exists="append",
        index=False,
    )
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.REAL_TIME_DIM_DATE_TABLE_NAME} "
            f"ADD PRIMARY KEY ({tp.DATE_COLUMN_NAME});"
        )


def split_date_to_hour_minute_second(data):
    date_col = data[[tp.DATE_COLUMN_NAME]]
    date_df = date_col.drop_duplicates(
        subset=tp.DATE_COLUMN_NAME, inplace=False
    ).reset_index(drop=True)

    date_df.insert(
        1,
        column=tp.HOUR_COLUMN_NAME,
        value=(date_df[tp.DATE_COLUMN_NAME] + timedelta(hours=7)).apply(
            lambda x: str(x)[11:13]
        ),
        allow_duplicates=False,
    )

    date_df.insert(
        2,
        column=tp.MINUTE_COLUMN_NAME,
        value=date_df[tp.DATE_COLUMN_NAME].apply(lambda x: str(x)[14:16]),
        allow_duplicates=False,
    )

    date_df.insert(
        3,
        column=tp.SECOND_COLUMN_NAME,
        value=date_df[tp.DATE_COLUMN_NAME].apply(lambda x: str(x)[17:19]),
        allow_duplicates=False,
    )

    return date_df


# DIM_SYMBOL table
def insert_data_into_dim_symbol_table(data):
    symbol_col = data[[tp.SYMBOL_COLUMN_ID]]
    symbol_df = symbol_col.drop_duplicates(subset=tp.SYMBOL_COLUMN_ID).reset_index(
        drop=True
    )
    symbol_df.insert(1, tp.SYMBOL_COLUMN_NAME, tp.GOLD_TICKER)

    connection = db.create_connection_database()
    symbol_df.to_sql(
        name=tp.DIM_SYMBOL_TABLE_NAME,
        con=connection,
        if_exists="append",
        index=False,
    )
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.DIM_SYMBOL_TABLE_NAME} "
            f"ADD PRIMARY KEY ({tp.SYMBOL_COLUMN_ID});"
        )


# FACT_GOLD_PRICE table
def insert_data_into_fact_gold_data_table(data):
    connection = db.create_connection_database()

    data.to_sql(
        name=tp.FACT_GOLD_DATA_TABLE_NAME,
        con=connection,
        if_exists="append",
        index=False,
    )
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.FACT_GOLD_DATA_TABLE_NAME} "
            f"ADD FOREIGN KEY ({tp.DATE_COLUMN_NAME}) REFERENCES dim_date({tp.DATE_COLUMN_NAME}), "
            f"ADD FOREIGN KEY ({tp.SYMBOL_COLUMN_ID}) REFERENCES dim_symbol({tp.SYMBOL_COLUMN_ID});"
        )


# REAL_TIME_FACT_GOLD_PRICE table
def insert_data_into_real_time_fact_gold_data_table(data):
    connection = db.create_connection_database()

    data.to_sql(
        name=tp.REAL_TIME_FACT_GOLD_DATA_TABLE_NAME,
        con=connection,
        if_exists="append",
        index=False,
    )
    with connection.connect() as con:
        con.execute(
            f"ALTER TABLE {tp.REAL_TIME_FACT_GOLD_DATA_TABLE_NAME} "
            f"ADD FOREIGN KEY ({tp.DATE_COLUMN_NAME}) REFERENCES real_time_dim_date({tp.DATE_COLUMN_NAME}), "
            f"ADD FOREIGN KEY ({tp.SYMBOL_COLUMN_ID}) REFERENCES dim_symbol({tp.SYMBOL_COLUMN_ID});"
        )


# insert data into DATABASE
def insert_data_into_database_from_beginning():
    data = crawling.crawl_data_in_current_year()
    real_time_data = crawling.crawl_data_every_minute()

    insert_data_into_dim_symbol_table(data)
    insert_data_into_dim_date_table(data)
    insert_data_into_fact_gold_data_table(data)

    insert_data_into_real_time_dim_date_table(real_time_data)
    insert_data_into_real_time_fact_gold_data_table(real_time_data)


def insert_data_in_database_everyday():
    data = crawling.crawl_data_every_day()

    insert_data_into_dim_date_table_everyday(data)
    insert_data_into_fact_gold_data_table(data)


# CALL insert functions
# insert_data_into_database_from_beginning()
insert_data_in_database_everyday()
