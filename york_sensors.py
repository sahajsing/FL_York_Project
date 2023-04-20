import requests
import xmltodict
import json
import pandas as pd
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import time
import os
from datetime import datetime

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

REGIONS = {"richmond-hill":rhill_boundary, "aurora":aurora_boundary, "markham": markham_boundary, "vaughan": vaughan_boundary, \
           "georgina": georgina_boundary, "newmarket": newmarket_boundary, "king-township": king_township_boundary, \
           "east-gwillimbury": east_gwillimbury_boundary, "whitchurch-stouffville":whitchurch_stouffville_boundary}

tests = ['test','test1','test2','testbales','baseline-warden','bview-407ebramp','aur-brcz-mosl','aur-ctr-wltn','highway48-bloomington',\
            'ninthline-407wb','donaldcousenspkwy-407eb','dufferin-countrydayschool','dufferin-407eb_caraway','highway27-407eb',\
            'aur-pkw-engl','aur-pkw-mary','jane-407wb','keele-407eb','kennedy-407eb','aur-mrry-kndy','leslie-407eb','lloydtown-aurora-400sb',\
                'markham-407wb','mccowan-407wb','aur-mrry-kndy','pinevalley-407wb','pvms-2','queensway-biscayne','warden-407wb','woodbine-407wb',\
                    'langstaffrde_yonge-407eb','lloydtown-aurora-400nb','weston-407wb','king-11thconcession','king-greenside',\
                        'keele-17thsideroad','keele-burtongrove_station','king-kingcitysecondary','king-kingoffices','king-parker','nashville-huntington']

def join_blank_spaces(location):
    # print(location)
    if " " in location:
        location_temp = location.split()
        for i in range(1,len(location_temp)+1,2):
            location_temp.insert(i,'-')
        location = "".join(location_temp)
    return location

def checkbounds(regions,coords):
    for region in regions: 
        if regions[region].contains(coords):
            return region
    return None

def get_sensor_info(filename, regions):
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
        # if " " in readerid:
        #     readerid_temp = readerid.split()
        #     for i in range(1,len(readerid_temp),2):
        #         readerid_temp.insert(i,'-')
        #     readerid = "".join(readerid_temp)
                
        latitude = sensor['geometry']['y']
        longitude = sensor['geometry']['x']
        regionName = checkbounds(regions,Point(latitude,longitude))
        data_to_store = {'OBJECTID':objectid,'FACILITYID':facilityid,'READERID':readerid,'Latitude':latitude,'Longitude':longitude,'Region':regionName}
        df.loc[len(df)] = data_to_store
    df.to_csv(filename,index=False)
