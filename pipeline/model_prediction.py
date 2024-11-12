import pandas as pd
from utils.database_utils import create_conn, create_connection_database
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

start_date = '2024-08-02'
end_date = '2024-09-02'
# load model
model = load_model('D:/thuantt2/Document/Big_data_project/model/my_model.h5')

# load data
conn = create_conn()
print(conn)
data = pd.read_sql_query(f"select date, close from fact_gold_data where date >= '{start_date}' and\
                         date <= '{end_date}'",con = conn)
scaler = MinMaxScaler(feature_range=(0,1))

scaled_data = scaler.fit_transform(data[['close']])
prediction=model.predict(scaled_data)
inverse_prediction = scaler.inverse_transform(prediction)
data['prediction'] = inverse_prediction

## Đưa dữ liệu vào databse
engine = create_connection_database()
print(engine)
data.to_sql(name="model_prediction", con=engine, if_exists = "append", index = False)