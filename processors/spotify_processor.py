from processors.event import Event
import os
import csv
from os import listdir
import json


SPOTIFY_DATA = {
    'total_play_time_ms': {
        'file_type': 'json',
        'file_start': 'StreamingHistory',
        'metric_key': lambda x: x['msPlayed'],
        'timestamp': lambda x: x['endTime'],
        'content': lambda x: ''
    },
    'distinct_songs_played': {
        'file_type': 'json',
        'file_start': 'StreamingHistory',
        'metric_key': lambda x: None,
        'timestamp': lambda x: x['endTime'],
        'content': lambda x: x['artistName'] + x['trackName']
    },
    'distinct_artists_played': {
        'file_type': 'json',
        'file_start': 'StreamingHistory',
        'metric_key': lambda x: None,
        'timestamp': lambda x: x['endTime'],
        'content': lambda x: x['artistName']
    },

}
def read_data(config):
    data_path = config['path']
    all_rows = []
    for key in SPOTIFY_DATA.keys():
        print('processing Spotify data for: ' + key)
        file_start = SPOTIFY_DATA[key]['file_start']
        metric_key = SPOTIFY_DATA[key]['metric_key']
        timestamp = SPOTIFY_DATA[key]['timestamp']
        content = SPOTIFY_DATA[key]['content']
        for path, folders, files in os.walk(data_path):
            for file in files:
                if file_start in file:
                    with open(path + '/' + file) as f:
                        reader = json.loads(f.read())
                        for row in reader:
                            event = Event(
                                source=config['source'],
                                metric_name=key,
                                metric_value=metric_key(row),
                                timestamp=timestamp(row),
                                content=content(row)
                            )
                            all_rows.append(event)
    return all_rows





