# Federated Learning in Edge Environments:
## An Implementation at the York Region-Based Bluetooth Traffic Monitoring System
--- 
Project for ECE1508 and ECE1548H. 

## Files

1. **main2_final.py:**
    - main python file to run all of the Federated Learning iterative process. 

2. **york_sensors.py:**
    - outlines the York regions and obtains the sensor information to store in __sensor_info.csv__

3. **ml_anaysis.py:**
    - Model training, predictions, errors and model aggregation (called by the central server)

4. **central_server.py:**
    - global model is initialized here and models are aggregated 

5. **edge_<region>.py:**
    - 9 identical edge region python files to simulate edge clients
    - Collect data, process, and compute a local model
    - check out **edge_aurora.py** for complete comments and details
  
6. Sample CSV files already uploaded of the stored data
    - 'traffic_data_<region>.csv' : sample traffic data for the region collected from real time data api
    - 'sensor_info.csv' : Bluetooth sensor information in csv from api
