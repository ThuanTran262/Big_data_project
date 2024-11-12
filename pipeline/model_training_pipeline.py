import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras import layers
from tensorflow.keras.optimizers import Adam

from utils.model_utils import get_x_y, train_test_split
from utils.database_utils import create_conn, create_connection_database
from params.model_params import N_DAY, ROOT_PATH

train_date = datetime.strftime(datetime.today(), '%Y-%m-%d')
print(f'Train date: {train_date}')

# load data
conn = create_conn()
print(conn)
data = pd.read_sql_query("select * from fact_gold_data",con = conn)

# data preprocessing
data = data[['date', 'close']]
data.index = data.pop('date')

scaler = MinMaxScaler(feature_range=(0,1))
scaled_data = scaler.fit_transform(data[["close"]])

x_data, y_data = get_x_y(scaled_data, n=N_DAY)
x_train, y_train, x_test, y_test = train_test_split(x_data, y_data)

# train model
model = Sequential([layers.Input((N_DAY, 1)), 
                    layers.LSTM(64),
                    layers.Dense(32, activation='relu'),
                    layers.Dense(32, activation='relu'),
                    layers.Dense(1)])
model.compile(loss='mse',
              optimizer=Adam(learning_rate=0.001),
              metrics=['mean_absolute_error'])
model.fit(x_train, y_train, batch_size = x_train.shape[1], epochs = 2)

# save model result to database
predictions=model.predict(x_test)
## inverse prediction & test:
inv_predictions = scaler.inverse_transform(predictions)
inv_y_test = scaler.inverse_transform(y_test)

## calculate the rmse
rmse = np.sqrt(np.mean((inv_predictions - inv_y_test)**2))
model_result = pd.DataFrame({'train_date': [train_date], 'rmse': [rmse]})

## Đưa dữ liệu vào databse
engine = create_connection_database()
print(engine)
model_result.to_sql(name="model_result", con=engine, if_exists = "append", index = False)

# save model
model.save(f'{ROOT_PATH}/model/my_model.h5', overwrite=True)