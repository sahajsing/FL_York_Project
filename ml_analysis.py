import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import preprocessing
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from sklearn.metrics import mean_squared_error
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import time
from datetime import datetime


global GLOBAL_MODEL
NUM_ESTIMATORS = 50

'''
    This function plots the actual values to predict labels.
'''
def plot_series(series_list,label_list=None,filename=None):
    
    if(len(series_list)==1):
        plt.plot(series_list[0])
        
    else:
        markers = ['-*', '-+', '-o', '-s', '-^', '-v', '-d', '-x']
        if(label_list is None):
            for i in range(len(series_list)):
                plt.plot(range(series_list[0].size), series_list[i], markers[i], label='time_series '+str(i))
        
        else:
            for i in range(len(series_list)):
                plt.plot(range(series_list[0].size), series_list[i], markers[i], label=label_list[i])
        
        plt.legend(loc='upper right', prop={'size': 14})
    
    plt.title(f'{filename}')
    plt.xlabel('index')
    plt.ylabel('value')
    plt.gcf().set_size_inches(16, 5)
    plt.show()
    plt.close()

'''
    These functions transform the timestamp '2023-04-11_19:39:49' 
    to seconds: 19*3600 + 39*60 + 49

    convtime: extracts the time portion of the timestamp.
    convtimetoseconds: performs the seconds calculation conversion.

    This is needed for the Machine Learning modelling - the RFModel
'''

def convtime(value):
    dt_obj = datetime.strptime(value, "%Y-%m-%d_%H:%M:%S")
    time_24 = dt_obj.strftime('%H:%M:%S')
    return time_24

def convtimetoseconds(timestamp):
    hour = int(timestamp.split(':')[0])
    minute = int(timestamp.split(':')[1])
    second = int(timestamp.split(':')[2])

    # convert to total number of seconds
    total_seconds = hour*3600 + minute*60 + second
    return total_seconds

'''
    This function retrieves the dataset stored for the region and converted the timestamp to seconds. 
'''
def get_data(filename):
    data = pd.read_csv(filename)
    data['timestamp'] = data['timestamp'].apply(lambda x: convtime(str(x)))
    data['timestamp'] = data['timestamp'].apply(lambda x: convtimetoseconds(str(x)))
    return data

'''
    This function splits the training and test data from the data collected.

    X: input features to the model
    y: output labels
'''
def prepare_data(data):
    X = data[['timestamp', 'origin_OBJECTID', 'dest_OBJECTID']]
    y = data['summary_samples']
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    return x_train, x_test, y_train, y_test

'''
    This function computes the RF Regression model with the training and test data.
    It also calculated the MSE and the NMSE and returns this value with the model.
'''

def RFmodel(x_train, x_test, y_train, y_test,reg=None,filename=None):
    t1 = time.time()

    if reg == None:
        reg = RandomForestRegressor(n_estimators=NUM_ESTIMATORS, max_features="auto", random_state=44)
    reg.fit(x_train, y_train)

    t2 = time.time()
    t = (t2-t1)
    print('{0:0.4f}'.format(t) + ' seconds for training MLP')

    # Predict test samples
    prd_rf = reg.predict(x_test)

    # Evaluate the model (calculate the test error MSE)
    mse_dif_rf = mean_squared_error(y_test, prd_rf)
    print('MSE (differnce): ' + str(mse_dif_rf))

    # Evaluate the model (calculate the test error NMSE)
    nmse_dif_rf = mse_dif_rf/np.var(y_test)
    print('NMSE (differnce): ' + str(nmse_dif_rf))

    # Plot the results
    plot_series([y_test, prd_rf], ['actual', 'predicted'],filename)

    return reg, nmse_dif_rf

'''
    This function predicts the data based on the existing dataset for the region for an already given model.
    This is mainly used to predict the values with shared global model.
'''    
def predict_data(x_test, y_test,model,filename=None):
    print('***** PREDICTING regional data from global model *****')
    # Predict test samples
    prd = model.predict(x_test)

    # Evaluate the model (calculate the test error MSE)
    mse_dif_rf = mean_squared_error(y_test, prd)
    print('MSE (differnce): ' + str(mse_dif_rf))

    # Evaluate the model (calculate the test error NMSE)
    nmse_dif_rf = mse_dif_rf/np.var(y_test)
    print('NMSE (differnce): ' + str(nmse_dif_rf))

    # Plot the results
    plot_series([y_test, prd], ['actual', 'predicted'],filename)

    return model, nmse_dif_rf


'''
    This function runs the complete model analysis with the data as the input and retunrs the model computed.
    If a global model is passed - it goes straight to the predictions with this model, otherwise
    computes the local regional edge model. 
'''
def run_model_analysis(filename,global_model=None,):
    data = get_data(filename)
    x_train, x_test, y_train, y_test = prepare_data(data)
    if global_model != None:
        print('Global model found!')
        print('--------------------')
        # reg, nmse = RFmodel(x_train, x_test, y_train, y_test,reg=global_model)
        reg,nmse = predict_data(x_test,y_test,global_model,filename=filename)
        return reg,nmse
    print('Computing regional model!')
    print('--------------------------')
    reg, nmse = RFmodel(x_train, x_test, y_train, y_test,reg=None,filename=filename)
    return reg,nmse 

'''
    This aggregates all the models from the edges 
        - global_model: current global model
        - edge_models: list of models of all edge clients
    
    ** Note ** 
        This is for a Random Forest model implementation.
        Each estimator in Random Forrest model is a decision tree classifier.
'''
def aggregate_models(global_model, edge_models):
    print(NUM_ESTIMATORS)
    number_of_edge_models = len(edge_models)
    global_model.n_estimators = number_of_edge_models*NUM_ESTIMATORS # 20 estimators in each model 
    
    print('-----------------------------------------------------------------------*************************')

    edge_estimators = []
    agg_edge_estimators =[]

    for edge_model in edge_models:
        edge_estimators.append(edge_model.estimators_)
    
    for estimator in edge_estimators:
        agg_edge_estimators.extend(estimator)

    global_model.estimators_ = agg_edge_estimators

    global GLOBAL_MODEL
    GLOBAL_MODEL = global_model
    return global_model

'''
    This function retrieves the model, namely for the initial global model to be computed. 
'''

def retrieve_model(filename):
    data = get_data(filename)
    x_train, x_test, y_train, y_test = prepare_data(data)
    reg, nmse = RFmodel(x_train, x_test, y_train, y_test,filename=filename) 

    return reg


