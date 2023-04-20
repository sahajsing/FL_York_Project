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

def join_blank_spaces(location):
    if " " in location:
        location_temp = location.split()
        for i in range(1,len(location_temp)+1,2):
            location_temp.insert(i,'-')
        location = "".join(location_temp)
    print
    return location

def check_file_created(filename):
    return os.path.isfile(filename)

def check_region_sensor(location_id):
    df = pd.read_csv('sensor_info.csv')

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
        sensor_record = df[df['READERID']==location_id]

        # check if no sensor avaialble
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


def get_traffic_data(data,filename, regionName):
    # url = "https://ww6.yorkmaps.ca/traveltime/iteris_traveltimes_out.xml"
    # response = requests.get(url)

    # data = response.content

    # convert xml to python dictionary
    xml_dict = xmltodict.parse(data)

    # Get traffic data in dictionary format
    traffic_data = xml_dict['match_summary_data']['match_summary']
    # extract the column headers for the data coming in
    columns = list(traffic_data[0].keys())

    # Create a dataframe with the appropriate columns
    df = pd.DataFrame(columns=columns)

    # Parse the traffic data and store in the pandas data frame
    # for i in range(len(traffic_data)):
    origin_region = []
    dest_region = []
    origin_OBJECTID = [] # sensor number used for ML training corresponding to orig and dest id
    dest_OBJECTID = []
    # tests = ['test','test1','test2','testbales','baseline-warden','bview-407ebramp','aur-brcz-mosl','aur-ctr-wltn','highway48-bloomington',\
    #          'ninthline-407wb','donaldcousenspkwy-407eb','dufferin-countrydayschool','dufferin-407eb_caraway','highway27-407eb',\
    #             'aur-pkw-engl','aur-pkw-mary','jane-407wb','keele-407eb','kennedy-407eb','aur-mrry-kndy','leslie-407eb','lloydtown-aurora-400sb',\
    #                 'markham-407wb','mccowan-407wb','aur-mrry-kndy','pinevalley-407wb','pvms-2','queensway-biscayne','warden-407wb','woodbine-407wb',\
    #                     'langstaffrde_yonge-407eb','lloydtown-aurora-400nb','weston-407wb']
    
    tests = york_sensors.tests
    for i in range(len(traffic_data)):
        data = list(traffic_data[i].values())
        data_to_store ={}
        if not any(test in tests for test in [data[1].lower(),data[2].lower()]):
            for j in range(len(columns)):
                columnName = columns[j]
                value = join_blank_spaces(data[j]).lower() if type(data[j]) == str else data[j]
                if columnName == 'speed_kph':
                    data_to_store[columnName] = value['#text']

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

            df.loc[len(df)] = data_to_store
        else:
            continue

    df['origin_region'] = origin_region
    df['dest_region'] = dest_region
    df['origin_OBJECTID'] = origin_OBJECTID
    df['dest_OBJECTID'] = dest_OBJECTID
    df = df.drop(['map_display','system_id','summary_mins','substitute_speed'],axis=1)
    df = df.drop(df[df['summary_samples']=='0'].index)
    print('Data retrieved and in dataframe!')
    
    df_region = df[df['origin_region'].isin([regionName]) | df['dest_region'].isin([regionName])]

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


def collect_data(data,filename):
    # while True: 
    get_traffic_data(data,filename, regionName='king-township')

        # send for training ! 
        
        # repeat every 15 mins -> data is updated every 15 minutes
        # time.sleep(15*60)

def retrieve_model(global_model,filename):

    # Run global model on regional dataset and check error
    gl_model,gl_nmse = ml_analysis.run_model_analysis(filename,global_model=global_model)
    print('\n')
    # Run and create new local modal on regional data and retrieve model and nmse
    region_model, region_nmse = ml_analysis.run_model_analysis(filename)

    # print(f'Global NMSE: {gl_nmse}')
    # print(f'Regional NMSE: {region_nmse}')
    print('\n')
    # Return the model that has the lower nmse (global vs. local regional)
    if region_nmse < gl_nmse:
        print('The local model is more accurate!')
        return region_model,region_nmse
    print('The shared global model is more accurate!')
    return global_model, gl_nmse

# if __name__ == "__main__":

#     # 1. Get sensor location information and store in sensor_info.csv
#     # get_sensor_info(regions)
#     filename = 'traffic_data_kingtownship.csv'
#     # get_traffic_data(filename)

#     while True: 
#         get_traffic_data(filename, regionName='king-township')

#         # send for training ! 
        
#         # repeat every 15 mins -> data is updated every 15 minutes
#         time.sleep(15*60)