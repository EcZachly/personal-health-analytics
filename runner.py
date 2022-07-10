import pandas as pd
from processors import fitbit_processor, linkedin_processor, spotify_processor
import timeline_augmenter

# Change to your username
FITBIT_NAME = 'ZacharyWilson'

processors = {
    'LinkedIn': {
        'processor': linkedin_processor,
        'path': 'data/linkedin'
    },
    'Fitbit': {
        'processor': fitbit_processor,
        'path': 'data/fitbit/' + FITBIT_NAME
    },
    'Spotify': {
        'processor': spotify_processor,
        'path': 'data/spotify/MyData'
    }
}

# I only want data after I started recording stuff with my fitbit
CUTOFF_DATE = '2019-06-01'

def do_daily_aggregates(input_csv_file, output_csv='output/daily_aggregates.csv'):
    print('starting daily aggregates step')
    avg_aggregates = ['resting_heart_rate', 'heart_rate_variability']
    count_distinct_aggregates = ['distinct_artists_played', 'distinct_songs_played']
    df = pd.read_csv(input_csv_file)
    df['date'] = pd.to_datetime(df['Timestamp'], utc=True).dt.floor('d')
    averages = df[df['Metric Name'].isin(avg_aggregates)].groupby(by=["Source", "Metric Name", 'date']).mean()
    count_distinct = df[df['Metric Name'].isin(count_distinct_aggregates)].groupby(by=["Source", "Metric Name", 'date'])['Content'].nunique().astype(float)
    sums = df[~df['Metric Name'].isin(avg_aggregates + count_distinct_aggregates)].groupby(by=["Source", "Metric Name", 'date']).sum().astype(float)
    count_filtered = count_distinct.to_frame()
    count_filtered = count_filtered.rename(columns={'Content': 'Metric Value'})
    combined = pd.concat([averages, sums, count_filtered])
    combined.to_csv(output_csv, mode='w')
    pivoted = combined.pivot_table(values='Metric Value', index=['date'], columns=['Metric Name']).reset_index()
    pivoted['str_date'] = pivoted['date'].astype(str)
    pivoted = timeline_augmenter.augment_daily_timelines(pivoted)
    # Only keep data where Fitbit is valid
    output = pivoted[pivoted['str_date'] > CUTOFF_DATE]
    output.to_csv('output/pivoted.csv', index=False)


def run_everything(csv_file='output/all_events_on_timeline.csv'):
    print('starting schema consolidation step')
    all_rows = []
    for key, processor_config in processors.items():
        processor = processor_config['processor']
        more_rows = processor.read_data({
            'path': processor_config['path'],
            'source': key
        })
        all_rows = all_rows + more_rows
    all_data = pd.DataFrame(all_rows, columns=['Source', 'Metric Name', 'Timestamp', 'Metric Value', 'Content'])
    all_data.to_csv(csv_file, index=False)
    do_daily_aggregates(input_csv_file=csv_file)


if __name__ == "__main__":
    run_everything()
