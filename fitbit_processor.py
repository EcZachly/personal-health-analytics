from event import Event
import os
import csv
from os import listdir
import json


def fix_fitbit_json_file_dates(x):
    return '20' + x['dateTime'][6:8] + '-' + x['dateTime'][0:2] + '-' + x['dateTime'][3:5] + ' ' + \
           x['dateTime'].split()[1]


# Change to your username
FITBIT_NAME = 'ZacharyWilson'
FITBIT_DATA_PATH = 'data/fitbit/' + FITBIT_NAME + '/'
FITBIT_SOURCE_NAME = 'Fitbit'

FITBIT_DATA = {
    'hrv': {
        'path_start': 'Sleep',
        'file_type': 'csv',
        'file_start': 'Heart Rate Variability Details',
        'metric_key': lambda x: x['rmssd'],
        'timestamp': lambda x: x['timestamp'],
    },
    'deep_sleep_minutes': {
        'path_start': 'Sleep',
        'file_type': 'csv',
        'file_start': 'sleep_score',
        'metric_key': lambda x: x['deep_sleep_in_minutes'],
        'timestamp': lambda x: x['timestamp'],
    },
    'active_zone_minutes': {
        'path_start': 'Physical Activity',
        'file_type': 'csv',
        'file_start': 'Active Zone Minutes',
        'metric_key': lambda x: x['total_minutes'],
        'timestamp': lambda x: x['date_time'],
    },
    'distance': {
        'path_start': 'Physical Activity',
        'file_type': 'json',
        'file_start': 'distance-',
        'metric_key': lambda x: x['value'],
        'timestamp': fix_fitbit_json_file_dates
    },
    'resting_heart_rate': {
        'path_start': 'Sleep',
        'file_type': 'csv',
        'file_start': 'sleep_score',
        'metric_key': lambda x: x['resting_heart_rate'],
        'timestamp': lambda x: x['timestamp'],
    },
    'restlessness': {
        'path_start': 'Sleep',
        'file_type': 'csv',
        'file_start': 'sleep_score',
        'metric_key': lambda x: x['restlessness'],
        'timestamp': lambda x: x['timestamp'],
    },
    'sleep_score': {
        'path_start': 'Sleep',
        'file_type': 'csv',
        'file_start': 'sleep_score',
        'metric_key': lambda x: x['overall_score'],
        'timestamp': lambda x: x['timestamp'],
    },
    'sleep_revitalization': {
        'path_start': 'Sleep',
        'file_type': 'csv',
        'file_start': 'sleep_score',
        'metric_key': lambda x: x['overall_score'],
        'timestamp': lambda x: x['timestamp'],
    },
    'steps': {
        'path_start': 'Physical Activity',
        'file_type': 'json',
        'file_start': 'steps-',
        'metric_key': lambda x: x['value'],
        'timestamp': fix_fitbit_json_file_dates
    },
    'stress_score': {
        'path_start': 'Stress',
        'file_type': 'csv',
        'file_start': 'Stress Score.csv',
        'metric_key': lambda x: x['STRESS_SCORE'],
        'timestamp': lambda x: x['DATE']
    }
}


def read_data():
    rows = []
    for key in FITBIT_DATA.keys():
        path_start = FITBIT_DATA[key]['path_start']
        for path, folders, files in os.walk(FITBIT_DATA_PATH + path_start):
            print('starting read for Fitbit data:' + key)
            path_start = FITBIT_DATA[key]['path_start']
            metric_key = FITBIT_DATA[key]['metric_key']
            timestamp = FITBIT_DATA[key]['timestamp']
            file_type = FITBIT_DATA[key]['file_type']
            files = listdir(path)
            file_start = FITBIT_DATA[key]['file_start']
            if path_start in path:
                for file in files:
                    if file_start in file:
                        with open(path + '/' + file) as f:
                            reader = csv.DictReader(f) if file_type == 'csv' else json.loads(f.read())
                            for row in reader:
                                metric_value = metric_key(row)
                                try:
                                    if float(metric_value) > 0.0:
                                        new_event = Event(source=FITBIT_SOURCE_NAME,
                                                          metric_name=key,
                                                          timestamp=timestamp(row),
                                                          metric_value=metric_key(row),
                                                          content=''
                                                          )
                                        rows.append(new_event)
                                except:
                                    print('error')
    return rows