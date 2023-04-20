import requests
import xmltodict
import json
import pandas as pd
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import time
import os
from datetime import datetime
import threading

import ml_analysis
import york_sensors
import central_server
import edge_aurora
import edge_eastgwillim
import edge_georgina
import edge_kingtownship
import edge_markham
import edge_newmarket
import edge_rhill
import edge_whitchstouff
import edge_vaughan

filename_aurora = 'traffic_data_aurora.csv'
filename_eastgwillim = 'traffic_data_eastgwillim.csv'
filename_georgina = 'traffic_data_georgina.csv'
filename_kingtownship = 'traffic_data_kingtownship.csv'
filename_markham = 'traffic_data_markham.csv'
filename_newmarket = 'traffic_data_newmarket.csv'
filename_rhill = 'traffic_data_rhill.csv'
filename_vaughan = 'traffic_data_vaughan.csv'
filename_whitchstouff = 'traffic_data_whitchstouff.csv'


'''
    This function retrieves the traffic data in raw XML format
'''
def get_traffic_data():
    url = "https://ww6.yorkmaps.ca/traveltime/iteris_traveltimes_out.xml"
    response = requests.get(url)

    data = response.content

    return data

if __name__  == '__main__':
        
    REGIONS = york_sensors.REGIONS
    print('Obtaining latest sensor information...')
    sensor_info = york_sensors.get_sensor_info('sensor_info.csv',REGIONS)
    print('Sensor info stored in: sensor_info.csv')
    
    print('\n')

    print('Obtaining the initial global shared mode;')
    global_model = central_server.retrieve_model('traffic_data_split.csv')
    ml_analysis.GLOBAL_MODEL = global_model
    print('Global shared model computed and distributed to edge clients')
    print('------------------------------------------------------------')
    print('\n')

    # add threading!! 

    while True:
        print('Beginning to collect data...')
        print('------------------------------------------------------------')
        print('\n')
        data = get_traffic_data()
        
        print('Collecting Aurora Data')
        edge_aurora.collect_data(data, filename_aurora)
        print('------------------------------------------------------------')

        print('Collecting East-Gwillimbury Data')
        edge_eastgwillim.collect_data(data,filename_eastgwillim)
        print('------------------------------------------------------------')

        print('Collecting Georgina Data')
        edge_georgina.collect_data(data,'traffic_data_georgina.csv')
        print('------------------------------------------------------------')

        print('Collecting King-Township Data')
        edge_kingtownship.collect_data(data,'traffic_data_kingtownship.csv')
        print('------------------------------------------------------------')

        print('Collecting Makrham Data')
        edge_markham.collect_data(data,'traffic_data_markham.csv')
        print('------------------------------------------------------------')

        print('Collecting Newmarket Data')
        edge_newmarket.collect_data(data,'traffic_data_newmarket.csv')
        print('------------------------------------------------------------')

        print('Collecting Richmond-Hill Data')
        edge_rhill.collect_data(data,'traffic_data_rhill.csv')
        print('------------------------------------------------------------')

        print('Collecting Whitchurch-Stouffville Data')
        edge_whitchstouff.collect_data(data,'traffic_data_whithcstouff.csv')
        print('------------------------------------------------------------')

        print('Collecting Vaughan Data')
        edge_vaughan.collect_data(data,'traffic_data_vaughan.csv')
        print('------------------------------------------------------------')



        print('------------------------------------------------------------')
        print('ALL REGIONAL DATA COLLECTED, COMPUTING MODELS')
        print('------------------------------------------------------------')
        print('\n')
        print('------------------------------------------------------------')
        print('CREATING and RETRIEVING LOCAL MODELS...') 
        print('------------------------------------------------------------')
        
        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing Aurora Model **********')
        print('\n')
        aurora_model,aurora_nmse = edge_aurora.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_aurora)
        print(f'Aurora model NMSE: {aurora_nmse}')
        print('------------------------------------------------------------')

        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing East-Gwillimbury Model **********')
        print('\n')
        eastgwillim_model,eastgwillim_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_eastgwillim)
        print(f'East-Gwillimbury model NMSE: {eastgwillim_nmse}')
        print('------------------------------------------------------------')

        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing Georgina Model **********')
        print('\n')
        georgina_model,georgina_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_georgina)
        print(f'Georgina model NMSE: {georgina_nmse}')
        print('------------------------------------------------------------')


        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing King-Township Model **********')
        print('\n')
        kingtownship_model, kingtownship_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_kingtownship)
        print(f'King-Township model NMSE: {kingtownship_nmse}')
        print('------------------------------------------------------------')

        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing Markham Model **********')
        print('\n')
        markham_model, markham_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_markham)
        print(f'Markham model NMSE: {markham_nmse}')
        print('------------------------------------------------------------')


        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing Newmarket Model **********')
        print('\n')
        newmarket_model, newmarket_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_newmarket)
        print(f'Newmarket model NMSE: {newmarket_nmse}')
        print('------------------------------------------------------------')

        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing Richmond-Hill Model **********')
        print('\n')
        rhill_model, rhill_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_rhill)
        print(f'Richmond-Hill model NMSE: {rhill_nmse}')
        print('------------------------------------------------------------')

        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing Whichurch-Stouffville Model **********')
        print('\n')
        whitchstouff_model,whitchstouff_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_whitchstouff)
        print(f'Whitchurch-Stouffville model NMSE: {whitchstouff_nmse}')
        print('------------------------------------------------------------')

        print('\n')
        print('------------------------------------------------------------')
        print('********** Computing Vaughan Model **********')
        print('\n')
        vaughan_model,vaughan_nmse = edge_eastgwillim.retrieve_model(ml_analysis.GLOBAL_MODEL,filename_vaughan)
        print(f'Vaughan model NMSE: {vaughan_nmse}')
        print('------------------------------------------------------------')

        
        print('\n')
        print('------------------------------------------------------------')
        print('AGGREGATING ALL LOCAL MODELS WITH NEW GLOBAL MODEL') 
        print('------------------------------------------------------------')
        ml_analysis.GLOBAL_MODEL = ml_analysis.aggregate_models(ml_analysis.GLOBAL_MODEL,[aurora_model,eastgwillim_model,georgina_model,\
                                                                                          kingtownship_model,markham_model,newmarket_model,rhill_model,\
                                                                                            whitchstouff_model,vaughan_model])
        print(f'Global Model n_estimators: {ml_analysis.GLOBAL_MODEL.n_estimators}')
        print(f'Length of global.estimators_: {len(ml_analysis.GLOBAL_MODEL.estimators_)}')
        print('------------------------------------------------------------')
  
        print('\n')
        print('------ 5 minutes until next data collection -----')
        print('\n')


        time.sleep(5*60)