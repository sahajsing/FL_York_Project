import requests
import xmltodict
import json
import pandas as pd
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import time
import os
from datetime import datetime
import ml_analysis
import york_sensors


'''
    This function preprocesses the data and joins any blank spaces for the orgin_id or dest_id.

    Example: Rutherford Rd --> Rutherford-Rd
'''
def join_blank_spaces(location):
    if " " in location:
        location_temp = location.split()
        for i in range(1,len(location_temp)+1,2):
            location_temp.insert(i,'-')
        location = "".join(location_temp)
    print
    return location

def collect_data(data,filename):
    # while True: 
    get_traffic_data(data,filename, regionName='aurora')
'''
    This function checks if a stored dataset is already created --> CSV.
'''
def check_file_created(filename):
    return os.path.isfile(filename)

'''
    This function checks if the sensor information data has been created. 
'''

def check_region_sensor(location_id):
    df = pd.read_csv('sensor_info.csv')

    ''' 
        These IF conditions below are outliers where the origin_id/dest_id in the traffic data set corresponding to a field device location 
        does not correspond to the exact name value in the sensor data information. 

        Example: weston-rutherford is read from the traffic data, however it is listed as weston-rutherford-rd in the sensor data information. 
    '''

    if location_id == 'weston-rutherford':
        sensor_record = df[df['READERID']=='weston-rutherford-rd']
        sensor_region = sensor_record['Region'].to_string(index=False)
        sensor_objectid = int(df.iloc[sensor_record.index[0],0])

    elif location_id == 'woodbine-19th':
        sensor_record = df[df['READERID']=='woodbine-19thave']
        sensor_region = sensor_record['Region'].to_string(index=False)
        sensor_objectid = int(df.iloc[sensor_record.index[0],0])

    elif location_id == 'keele-lloydtown':
        sensor_record = df[df['READERID']=='lloydtown-janescurve']
        sensor_region = sensor_record['Region'].to_string(index=False)
        sensor_objectid = int(df.iloc[sensor_record.index[0],0])

    elif location_id == 'yonge-bantry':
        sensor_record = df[df['READERID']=='yonge-bantry_scott']
        sensor_region = sensor_record['Region'].to_string(index=False)
        sensor_objectid = int(df.iloc[sensor_record.index[0],0])

    else:
        # for cases where it is not an exception to the IF conditions above
        sensor_record = df[df['READERID']==location_id]

        # check if no sensor avaialble - sometimes data source locations don't match any sensor field device, these are ruled out here
        if (int(sensor_record.size) == 0):
            print(f'{location_id} = {sensor_record.size}')
            print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
            sensor_region = 'invalid'
            sensor_objectid = -1
            return sensor_region, sensor_objectid
        
        sensor_region = sensor_record['Region'].to_string(index=False)

        # get the integer type of the associated OBJECTID aka SENSORID by accessing the matched record row # and the FACILITYID column(=1)
        sensor_objectid = int(df.iloc[sensor_record.index[0],0])

    # exception cases where the sensor data label for the location does not match with corresponding label in traffic data
    # example: 'weston-rutherford -> weston-rutherfordrd' or 'woodbine-19th' -> 'woodbine-19thave'
    return sensor_region, sensor_objectid

'''
    This function takes the data (retrieved from the API requrest), filename to store the data and the regionName to filter the data
    to parse, preprocess and store the data for modelling. 

'''
def get_traffic_data(data,filename, regionName):

    # convert xml to python dictionary
    xml_dict = xmltodict.parse(data)

    # Get traffic data in dictionary format
    traffic_data = xml_dict['match_summary_data']['match_summary']
    # extract the column headers for the data coming in
    columns = list(traffic_data[0].keys())

    # Create a dataframe with the appropriate columns
    df = pd.DataFrame(columns=columns)

    origin_region = [] # associated values of each field orgin_id traffic sample to the right region
    dest_region = [] # associated values of each field dest_id traffic sample to the right region
    
    # sensor ID number used for ML training corresponding to origin and dest id
    origin_OBJECTID = [] 
    dest_OBJECTID = []

    # these are outliers present in the traffic data that don't correspond to any sensor 
    tests = york_sensors.tests

    # Parses the retrieved traffic data from the API 
    for i in range(len(traffic_data)):
        data = list(traffic_data[i].values())
        data_to_store ={}

        # ensure the traffic data is not in the test outliers and is converted to all lower case if string type
        if not any(test in tests for test in [data[1].lower(),data[2].lower()]):
            for j in range(len(columns)):
                columnName = columns[j]
                value = join_blank_spaces(data[j]).lower() if type(data[j]) == str else data[j]

                # The following columns/features of the traffic data require further processing
                if columnName == 'speed_kph':
                    data_to_store[columnName] = value['#text']

                # For orgin_id and dest_id --> the associated region and sensor ID needs to be retrieved and stored
                    # Region will be used to filter by each edge
                    # objectid will be used for Machine Learning input to the model 

                elif columnName =='origin_id' and value not in tests:
                    region_id,objectid = check_region_sensor(value.lower())
                    origin_region.append(region_id)
                    origin_OBJECTID.append(objectid)
                    data_to_store[columnName] = value.lower()

                elif columnName =='dest_id' and value not in tests:
                    region_id, objectid = check_region_sensor(value.lower())
                    dest_region.append(region_id)
                    dest_OBJECTID.append(objectid)
                    data_to_store[columnName] = value.lower()
                
                elif columnName == 'timestamp':
                    # timestamp format: '4/11/2023-7:39:49-pm'
                    # need to convert to 24 hour time: 4/11/2023-19:39:49
                    dt_obj = datetime.strptime(value,'%m/%d/%Y-%I:%M:%S-%p') # output = '2023-04-11 19:39:49'
                    date_24 = dt_obj.strftime('%Y-%m-%d')
                    time_24 = dt_obj.strftime('%H:%M:%S')
                    timestamp = date_24 + '_' + time_24
                    data_to_store[columnName] = timestamp

                else:
                    data_to_store[columnName] = value

            # store the new row of data to the pandas data frame
            df.loc[len(df)] = data_to_store
        else:
            continue
    
    # store the region and sensor ID data as new columns in the pandas dataframe of traffic data
    df['origin_region'] = origin_region
    df['dest_region'] = dest_region
    df['origin_OBJECTID'] = origin_OBJECTID
    df['dest_OBJECTID'] = dest_OBJECTID
    
    # drop unnecessary data columns
    df = df.drop(['map_display','system_id','summary_mins','substitute_speed'],axis=1)
    df = df.drop(df[df['summary_samples']=='0'].index)
    print('Data retrieved and in dataframe!')

    # filter the data by each region
    df_region = df[df['origin_region'].isin([regionName]) | df['dest_region'].isin([regionName])]

    # append the data if a file to store the data is already created, otherwise a new one is created 
    if check_file_created(filename):
        print(f'Data already exists in {filename}. Merging the new data with the existing data!')
        df_existing = pd.read_csv(filename)
        df_merged = pd.concat([df_existing,df_region],ignore_index=True)
        print(f'Done Merging! {datetime.now()}')
        df_merged.to_csv(filename,index=False)
        return df_merged
    else:
        df_region.to_csv(filename, encoding='utf-8', index=False)
        print(f'Data stored in {filename}!')
        return df_region


'''
    This function computes the local model for the region and compares it to the global model.

    The better model is returned. 
'''
def retrieve_model(global_model,filename):

    # Run global model on regional dataset and check error
    gl_model,gl_nmse = ml_analysis.run_model_analysis(filename,global_model=global_model)
    print('\n')
    # Run and create new local modal on regional data and retrieve model and nmse
    region_model, region_nmse = ml_analysis.run_model_analysis(filename)

    print('\n')
    # Return the model that has the lower nmse (global vs. local regional)
    if region_nmse < gl_nmse:
        print('The local model is more accurate!')
        return region_model,region_nmse
    print('The shared global model is more accurate!')
    return global_model, gl_nmse

