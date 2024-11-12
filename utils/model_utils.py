import numpy as np

def get_x_y(data_array, n=100):
    """
    Returns the predictors array and dependent variables array
    
    params:
            data_array:
            n: number of previous days that are used for predicting the current day
    """
    x_data = []
    y_data = []
    for i in range(n, len(data_array)-1):
        x_data.append(data_array[(i-n):i])
        y_data.append(data_array[i+1])
    x_data, y_data = np.array(x_data), np.array(y_data)
    return x_data, y_data


def train_test_split(x, y, r=0.8):
    """
    Returns the train and test array

    params:
    x: predictors
    y: dependent variable
    r: rate
    """
    splitting_len = int(len(x)*r)
    x_train = x[:splitting_len]
    y_train = y[:splitting_len]
    x_test = x[splitting_len:]
    y_test = y[splitting_len:]
    return x_train, y_train, x_test, y_test