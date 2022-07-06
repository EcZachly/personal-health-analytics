import fitbit_processor
import linkedin_processor
import timeline_augmenter
import pandas as pd
import csv

processors = {
    'fitbit': fitbit_processor,
    'linkedin': linkedin_processor
}

# I only want data after I started recording stuff with my fitbit
CUTOFF_DATE = '2019-06-01'

def run_everything(csv_file='output/all_events_on_timeline.csv'):
    print('starting schema consolidation step')
    all_rows = []
    for key, processor in processors.items():
        more_rows = processor.read_data()
        all_rows = all_rows + more_rows
    with open(csv_file, 'w') as f:
        w = csv.writer(f)
        w.writerow(('Source', 'Metric Name', 'Timestamp', 'Metric Value', 'Content'))
        for data in all_rows:
            w.writerow((data.source, data.metric_name, data.timestamp, float(data.metric_value),
                        ' '.join(data.content.split())))
    do_daily_aggregates(input_csv_file=csv_file)


def do_daily_aggregates(input_csv_file, output_csv='output/daily_aggregates.csv'):
    print('starting daily aggregates step')
    avg_aggregates = ['hr', 'hrv']
    df = pd.read_csv(input_csv_file)
    df['date'] = pd.to_datetime(df['Timestamp'], utc=True).dt.floor('d')
    averages = df[df['Metric Name'].isin(avg_aggregates)].groupby(by=["Source", "Metric Name", 'date']).mean()
    sums = df[~df['Metric Name'].isin(avg_aggregates)].groupby(by=["Source", "Metric Name", 'date']).sum()
    combined = pd.concat([averages, sums])
    combined.to_csv(output_csv, mode='w')
    pivoted = combined.pivot_table(values='Metric Value', index=['date'], columns=['Metric Name']).reset_index()
    pivoted['str_date'] = pivoted['date'].astype(str)
    pivoted = timeline_augmenter.augment_daily_timelines(pivoted)
    # Only keep data where Fitbit is valid
    output = pivoted[pivoted['str_date'] > CUTOFF_DATE]
    output.to_csv('output/pivoted.csv')


run_everything()
