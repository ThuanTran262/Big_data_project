import pandas as pd
import numpy as np
from datetime import datetime
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

import sys
sys.path.insert(0, "/home/thuantt/airflow/Big_data_project")

from params.model_params import *
from utils.date_helper_utils import get_date_range, get_lag_nday_excl_weekend
from utils.database_utils import create_conn, create_connection_database


def model_prediction(report_date):
    next_date = get_date_range(report_date, 0, 1)[0]

    # load model
    model = load_model(f'{ROOT_PATH}/model/my_model.keras')

    # load data
    conn = create_conn()
    data = pd.read_sql_query(f"select date, close, row_number() over(partition by symbol order by date desc) rn \
                              from fact_gold_data where date(date) <= '{report_date}'", con=conn)
    data = data.query(f'rn <= {N_DAY}')[['date', 'close']]

    # data preprocessing
    scaler = MinMaxScaler(feature_range=(0,1))
    scaled_data = scaler.fit_transform(data[['close']])

    # Predict
    x_data = np.array([scaled_data])
    prediction=model.predict(x_data)
    print(prediction)

    # Inverse result
    inverse_prediction = scaler.inverse_transform(prediction)
    print(inverse_prediction.reshape(-1))
    result = pd.DataFrame({'date': [next_date], 'predictions': inverse_prediction.reshape(-1)})

    ## Đưa dữ liệu vào databse
    engine = create_connection_database()
    print(engine)
    result.to_sql(name="model_prediction", con=engine, if_exists = "append", index = False)