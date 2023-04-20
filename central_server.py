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

count = 0
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
        # print(f'{sensor_record.index[0]}')
        sensor_objectid = int(df.iloc[sensor_record.index[0],0])

    # exception cases where the sensor data label for the location does not match with corresponding label in traffic data
    # example: 'weston-rutherford -> weston-rutherfordrd' or 'woodbine-19th' -> 'woodbine-19thave'
    return sensor_region, sensor_objectid


def get_traffic_data(filename):
    url = "https://ww6.yorkmaps.ca/traveltime/iteris_traveltimes_out.xml"
    response = requests.get(url)

    data = response.content

    # convert xml to python dictionary
    xml_dict = xmltodict.parse(data)

    # Get traffic data in dictionary format
    traffic_data = xml_dict['match_summary_data']['match_summary']
    # extract the column headers for the data coming in
    columns = list(traffic_data[0].keys())

    # Create a dataframe with the appropriate columns
    df = pd.DataFrame(columns=columns)

    # Parse the traffic data and store in the pandas data frame
    origin_region = []
    dest_region = []
    origin_OBJECTID = [] # sensor number used for ML training corresponding to orig and dest id
    dest_OBJECTID = []
    tests = ['test','test1','test2','testbales','baseline-warden','bview-407ebramp','aur-brcz-mosl','aur-ctr-wltn','highway48-bloomington',\
             'ninthline-407wb','donaldcousenspkwy-407eb','dufferin-countrydayschool','dufferin-407eb_caraway','highway27-407eb',\
                'aur-pkw-engl','aur-pkw-mary','jane-407wb','keele-407eb','kennedy-407eb','aur-mrry-kndy','leslie-407eb','lloydtown-aurora-400sb',\
                    'markham-407wb','mccowan-407wb','aur-mrry-kndy','pinevalley-407wb','pvms-2','queensway-biscayne','warden-407wb','woodbine-407wb',\
                        'langstaffrde_yonge-407eb','lloydtown-aurora-400nb','weston-407wb']
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
    if check_file_created(filename):
        print(f'Data already exists in {filename}. Merging the new data with the existing data!')
        df_existing = pd.read_csv(filename)
        df_merged = pd.concat([df_existing,df],ignore_index=True)
        print(f'Done Merging! {datetime.now()}')
        df_merged.to_csv(filename,index=False)
    else:
        df.to_csv(filename, encoding='utf-8', index=False)
        print(f'Data stored in {filename}!')



def checkbounds(regions,coords):
    for region in regions: 
        if regions[region].contains(coords):
            return region
    return None
            

# define region boundaries
markham_coords = [(43.798040, -79.420198),(43.834744, -79.428822),(43.846940, -79.371010),(43.923765, -79.389469),(43.963416, -79.217711),(43.855474, -79.170244)]
markham_boundary = Polygon(markham_coords)

rhill_coords = [(43.957145, -79.485633),(43.977711, -79.392730),(43.846874, -79.370797),(43.829539, -79.454052)]
rhill_boundary = Polygon(rhill_coords)

vaughan_coords = [(43.749782, -79.639519),(43.876371, -79.711929), (43.924287, -79.477759),(43.957145, -79.485633),(43.834744, -79.428822), (43.798057, -79.420026)]
vaughan_boundary = Polygon(vaughan_coords)

king_township_coords = [(43.874482, -79.710796),(43.989786, -79.775515),(44.020329, -79.658260),(44.048933, -79.606536),(44.093837, -79.556229),\
                       (44.150700, -79.531797),(43.924416, -79.477831)]
king_township_boundary = Polygon(king_township_coords)

aurora_coords = [(43.957139, -79.485671),(44.016694, -79.499727),(44.034979, -79.412588),(43.977676, -79.393110)]
aurora_boundary = Polygon(aurora_coords)

newmarket_coords = [(44.016734, -79.499678),(44.063559, -79.509836),(44.083819, -79.423001),(44.035275, -79.412636)]
newmarket_boundary = Polygon(newmarket_coords)

east_gwillimbury_coords = [(44.063914, -79.509958),(44.150595, -79.531918),(44.181768, -79.517803),(44.223317, -79.327997),(44.101289, -79.276154),\
                           (44.068355, -79.420759),(44.083839, -79.422984)]
east_gwillimbury_boundary = Polygon(east_gwillimbury_coords)

whitchurch_stouffville_coords = [(43.923665, -79.390061),(44.068353, -79.420757),(44.101273, -79.276175),(43.963973, -79.215728)]
whitchurch_stouffville_boundary = Polygon(whitchurch_stouffville_coords)

georgina_coords = [(44.181736, -79.516964),(44.269120, -79.505900),(44.311058, -79.480292),(44.357412, -79.197955),(44.262742, -79.155903)]
georgina_boundary = Polygon(georgina_coords)

regions = {"richmond-hill":rhill_boundary, "aurora":aurora_boundary, "markham": markham_boundary, "vaughan": vaughan_boundary, \
           "georgina": georgina_boundary, "newmarket": newmarket_boundary, "king-township": king_township_boundary, \
           "east-gwillimbury": east_gwillimbury_boundary, "whitchurch-stouffville":whitchurch_stouffville_boundary}


def get_sensor_info(regions):
    '''
        This function retrieves the locations of the sensors in York Region.
            - READERID: Intersection the sensor is located
            - x: longitude
            - y: latitude
    '''
    url = "https://ww8.yorkmaps.ca/arcgis/rest/services/OpenData/Traffic/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json"
    response = requests.get(url)
    sensor_info_jsonstr = json.dumps(response.json())
    sensor_info_jsondict = json.loads(sensor_info_jsonstr)['features']
    columns = ['OBJECTID','FACILITYID','READERID','Latitude','Longitude','Region']
    df = pd.DataFrame(columns=columns)
    for sensor in sensor_info_jsondict:
        objectid = sensor['attributes']['OBJECTID']
        facilityid = sensor['attributes']['FACILITYID']
        
        readerid = join_blank_spaces(sensor['attributes']['READERID'].lower())
                
        latitude = sensor['geometry']['y']
        longitude = sensor['geometry']['x']
        regionName = checkbounds(regions,Point(latitude,longitude))
        data_to_store = {'OBJECTID':objectid,'FACILITYID':facilityid,'READERID':readerid,'Latitude':latitude,'Longitude':longitude,'Region':regionName}
        df.loc[len(df)] = data_to_store
    df.to_csv('sensor_info.csv',index=False)



def retrieve_model(filename):
    global_model = ml_analysis.retrieve_model(filename)
    return global_model

