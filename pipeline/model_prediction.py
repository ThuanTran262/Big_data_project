import pandas as pd
from utils.database_utils import create_conn, create_connection_database
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

# load model
model = load_model('my_model.h5')

# load data
# load data
conn = create_conn()
print(conn)
data = pd.read_sql_query("select Date, Close from fact_gold_data",con = conn)
scaler = MinMaxScaler(feature_range=(0,1))

scaled_data = scaler.fit_transform(data[['Close']])
prediction=model.predict(scaled_data)
inverse_prediction = scaler.inverse_transform(prediction)
data['Prediction'] = inverse_prediction

## Đưa dữ liệu vào databse
engine = create_connection_database()
print(engine)
data.to_sql(name="model_rediction", con=engine, if_exists = "append", index = False)