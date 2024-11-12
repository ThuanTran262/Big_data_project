import pandas as pd
from datetime import datetime
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

from params.model_params import *
from utils.date_helper import get_date_range
from utils.database_utils import create_conn, create_connection_database


report_date = datetime.strftime(datetime.today(), '%Y-%m-%d')
start_date = get_date_range(report_date, 30, 0)[0]
end_date = get_date_range(report_date, 1, 0)[0]
next_date = get_date_range(report_date, 0, 1)[-1]


# load model
model = load_model(f'{ROOT_PATH}/model/my_model.h5')

# load data
conn = create_conn()
print(conn)
data = pd.read_sql_query(f"select date, close from fact_gold_data where date >= '{start_date}' and\
                         date <= '{end_date}'",con = conn)

# data preprocessing
scaler = MinMaxScaler(feature_range=(0,1))
scaled_data = scaler.fit_transform(data[['close']])

# Predict
x_data = [scaled_data]
prediction=model.predict(x_data)

# Inverse result
inverse_prediction = scaler.inverse_transform(prediction)
result = pd.DataFrame({'date': [next_date], 'predictions': inverse_prediction.reshape(-1)})

## Đưa dữ liệu vào databse
engine = create_connection_database()
print(engine)
result.to_sql(name="model_prediction", con=engine, if_exists = "append", index = False)