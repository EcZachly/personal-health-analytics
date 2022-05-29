import csv
import datetime
from os import listdir, walk
from collections import namedtuple
from dateutil import parser
import pandas as pd
import json


Event = namedtuple("Event", "source metric_name timestamp metric_value content")
# Update this with your Fitbit name after you download the script
YOUR_FITBIT_NAME = 'ZacharyWilson'


LINKEDIN_DATA = {
    'shares': {
        'path': 'data/linkedin/Shares.csv',
        'file_type': 'csv',
        'metric_key': lambda x: 1,
        'content_key': lambda x: x['ShareCommentary']
    },
    'comments': {
        'path': 'data/linkedin/Comments.csv',
        'file_type': 'csv',
        'metric_key': lambda x: 1,
        'content_key': lambda x: x['Message']
    },
    'reactions': {
        'path': 'data/linkedin/Reactions.csv',
        'file_type': 'csv',
        'metric_key': lambda x: 1,
        'content_key': lambda x: x['Type']
    }
}


def fix_fitbit_json_file_dates(x):
    return '20' + x['dateTime'][6:8] + '-' + x['dateTime'][0:2] + '-' + x['dateTime'][3:5] + ' ' + \
           x['dateTime'].split()[1]


FITBIT_DATA = {
    'hrv': {
        'path': f'data/fitbit/{YOUR_FITBIT_NAME}/Sleep/',
        'file_type': 'csv',
        'file_start': 'Heart Rate Variability Details',
        'metric_key': lambda x: x['rmssd'],
        'timestamp': lambda x: x['timestamp'],
    },
    'active_zone_minutes': {
        'path': f'data/fitbit/{YOUR_FITBIT_NAME}/Physical Activity',
        'file_type': 'csv',
        'file_start': 'Active Zone Minutes',
        'metric_key': lambda x: x['total_minutes'],
        'timestamp': lambda x: x['date_time'],
    },
    'hr': {
        'path': f'data/fitbit/{YOUR_FITBIT_NAME}/Physical Activity',
        'file_type': 'json',
        'file_start': 'heart_rate-',
        'metric_key': lambda x: x['value']['bpm'] if 'bpm' in x['value'] else -1,
        'timestamp': fix_fitbit_json_file_dates
    },
    'distance': {
        'path': f'data/fitbit/{YOUR_FITBIT_NAME}/Physical Activity',
        'file_type': 'json',
        'file_start': 'distance-',
        'metric_key': lambda x: x['value'],
        'timestamp': fix_fitbit_json_file_dates
    },
    'steps': {
        'path': f'data/fitbit/{YOUR_FITBIT_NAME}/Physical Activity',
        'file_type': 'json',
        'file_start': 'steps-',
        'metric_key': lambda x: x['value'],
        'timestamp': fix_fitbit_json_file_dates
    },
    'stress_score': {
        'path': f'data/fitbit/{YOUR_FITBIT_NAME}/Stress',
        'file_type': 'csv',
        'file_start': 'Stress Score.csv',
        'metric_key': lambda x: x['STRESS_SCORE'],
        'timestamp': lambda x: x['DATE']
    }
}


def datetime_valid(dt_str):
    try:
        parser.parse(dt_str)
    except:
        return False
    return True


def read_linkedin_data():
    rows = []
    for key in LINKEDIN_DATA.keys():
        print('starting read for LinkedIn data:' + key)
        path = LINKEDIN_DATA[key]['path']
        content_key = LINKEDIN_DATA[key]['content_key']
        metric_key = LINKEDIN_DATA[key]['metric_key']
        file_type = LINKEDIN_DATA[key]['file_type']
        with open(path) as f:
            reader = csv.DictReader(f, dialect='excel') if file_type == 'csv' else json.loads(f.read())
            for row in reader:
                if 'Date' in row and datetime_valid(row['Date']):
                    new_event = Event(
                        source='LinkedIn',
                        metric_name=key,
                        timestamp=row['Date'],
                        metric_value=metric_key(row),
                        content=content_key(row)
                    )
                    rows.append(new_event)
                elif 'Date' in row:
                    combined_content = rows[-1].content + ' ' + row['Date']
                    rows[-1] = Event(
                        source='LinkedIn',
                        metric_name=rows[-1].metric_name,
                        timestamp=rows[-1].timestamp,
                        metric_value=metric_key(row),
                        content=combined_content
                    )
    return rows


def read_fitbit_data():
    rows = []
    for key in FITBIT_DATA.keys():
        print('starting read for Fitbit data:' + key)
        path = FITBIT_DATA[key]['path']
        metric_key = FITBIT_DATA[key]['metric_key']
        timestamp = FITBIT_DATA[key]['timestamp']
        file_type = FITBIT_DATA[key]['file_type']
        files = listdir(path)
        file_start = FITBIT_DATA[key]['file_start']
        for file in files:
            if file_start in file:
                with open(path + '/' + file) as f:
                    reader = csv.DictReader(f) if file_type == 'csv' else json.loads(f.read())
                    for row in reader:
                        metric_value = metric_key(row)
                        if float(metric_value) > 0.0:
                            new_event = Event(source='Fitbit',
                                              metric_name=key,
                                              timestamp=timestamp(row),
                                              metric_value=metric_key(row),
                                              content=''
                                              )
                            rows.append(new_event)
    return rows


def run_everything():
    print('starting schema consolidation step')
    fitbit_rows = read_fitbit_data()
    linkedin_rows = read_linkedin_data()
    all_rows = fitbit_rows + linkedin_rows
    csv_file = 'output/all_events_on_timeline.csv'
    with open(csv_file, 'w') as f:
        w = csv.writer(f)
        w.writerow(('Source', 'Metric Name', 'Timestamp', 'Metric Value', 'Content'))
        for data in all_rows:
            w.writerow((data.source, data.metric_name, data.timestamp, float(data.metric_value), ' '.join(data.content.split())))
    do_daily_aggregates(csv_file=csv_file)


def do_daily_aggregates(csv_file):
    print('starting daily aggregates step')
    avg_aggregates = ['hr', 'hrv']
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['Timestamp']).dt.floor('d')
    averages = df[df['Metric Name'].isin(avg_aggregates)].groupby(by=["Source", "Metric Name", 'date']).mean()
    sums = df[~df['Metric Name'].isin(avg_aggregates)].groupby(by=["Source", "Metric Name", 'date']).sum()
    output_file = 'output/daily_aggregates.csv'
    combined = pd.concat([averages, sums])
    combined.to_csv(output_file, mode='w')


run_everything()

