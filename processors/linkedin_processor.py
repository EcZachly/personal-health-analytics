from processors.event import Event
from dateutil import parser
import os
import csv
import json


LINKEDIN_DATA = {
    'shares': {
        'file_start': 'Shares.csv',
        'file_type': 'csv',
        'metric_key': lambda x: 1,
        'content_key': lambda x: x['ShareCommentary']
    },
    'comments_given': {
        'file_start': 'Comments.csv',
        'file_type': 'csv',
        'metric_key': lambda x: 1,
        'content_key': lambda x: x['Message']
    },
    'reactions_given': {
        'file_start': 'Reactions.csv',
        'file_type': 'csv',
        'metric_key': lambda x: 1,
        'content_key': lambda x: x['Type']
    },
    'reactions_received': {
        'file_start': 'all_linkedin_posts.json',
        'file_type': 'json',
        'metric_key': lambda x: x['reactions'],
        'content_key': lambda x: ''
    },
    'comments_received': {
        'file_start': 'all_linkedin_posts.json',
        'file_type': 'json',
        'metric_key': lambda x: x['comments'],
        'content_key': lambda x: ''
    }
}


def datetime_valid(dt_str):
    try:
        parser.parse(dt_str)
    except:
        return False
    return True


def read_data(config):
    rows = []
    data_path = config['path']
    for key in LINKEDIN_DATA.keys():
        print('processing Linkedin data for:' + key)
        file_start = LINKEDIN_DATA[key]['file_start']
        content_key = LINKEDIN_DATA[key]['content_key']
        metric_key = LINKEDIN_DATA[key]['metric_key']
        file_type = LINKEDIN_DATA[key]['file_type']
        for path, folders, files in os.walk(data_path):
            for file in files:
                full_path = path + '/' + file
                if file_start == file:
                    with open(full_path) as f:
                        reader = csv.DictReader(f, dialect='excel') if file_type == 'csv' else json.loads(f.read())
                        for row in reader:
                            # Sometimes the rows don't have a date, so we just try to parse it
                            content_value = content_key(row) or ''
                            content_value = ' '.join(content_value.split())
                            if 'Date' in row and datetime_valid(row['Date']):
                                new_event = Event(
                                    source=config['source'],
                                    metric_name=key,
                                    timestamp=row['Date'],
                                    metric_value=float(metric_key(row)),
                                    content=content_value
                                )
                                rows.append(new_event)
                            # If Date fails to parse, that means this content has a return/enter in it
                            # Let's keep adding more of the content to the previous row
                            elif 'Date' in row:
                                combined_content = rows[-1].content + ' ' + ' '.join(row['Date'])
                                rows[-1] = Event(
                                    source=config['source'],
                                    metric_name=rows[-1].metric_name,
                                    timestamp=rows[-1].timestamp,
                                    metric_value=float(metric_key(row)),
                                    content=combined_content
                                )
    return rows