TIMELINES = {
    'employer': [
        ('Facebook', ['2016-08-22', '2018-06-01']),
        ('Netflix', ['2018-06-01', '2020-03-06']),
        ('Unemployed', ['2020-03-06', '2021-02-22']),
        ('Airbnb', ['2021-02-22', '2022-12-31'])
    ],
    'location': [
        ('San Francisco', ['2019-06-01', '2020-03-06']),
        ('Puerto Rico', ['2020-03-06', '2020-06-01']),
        ('Utah', ['2020-06-01', '2020-07-10']),
        ('Road Trip', ['2020-07-10', '2020-09-01']),
        ('San Francisco', ['2020-09-01', '2021-01-15']),
        ('Utah', ['2021-01-15', '2021-09-01']),
        ('San Francisco', ['2021-09-01', '2022-12-31'])
    ],
    'relationship_status': [
        ('Single', ['2019-06-01', '2019-10-15']),
        ('In Relationship', ['2019-10-15', '2021-01-15']),
        ('Single', ['2021-01-15', '2021-09-27']),
        ('In Relationship', ['2021-09-27', '2022-01-06']),
        ('Single', ['2022-01-06', '2022-12-31'])
    ]
}


def augment_timeline(df, column_name, timeline):
    df[column_name] = df.apply(
        lambda row: next(iter([e[0] for e in timeline if e[1][0] <= row['str_date'] < e[1][1]]), 'unknown'), axis=1)
    return df


def augment_daily_timelines(df):
    for key in TIMELINES.keys():
        df = augment_timeline(df, key, TIMELINES[key])
    return df
