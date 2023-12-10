import pandas as pd
from datetime import timedelta
from datetime import datetime


def min_time_diff(group):
    min_diff = float('inf')  # Set an initial maximum value for minimum difference
    times = group['arrival'].tolist()

    for i in range(len(times)):
        for j in range(i + 1, len(times)):
            time_diff = abs((datetime.combine(datetime.today(), times[j]) -
                             datetime.combine(datetime.today(),
                                              times[i])).total_seconds())
            if min_diff > time_diff:
                min_diff = time_diff
    return min_diff


# Define a custom aggregation function to collect the values in to a list
def list_agg(x):
    return list(x)


def str_to_date(s):
    return datetime.strptime(s, '%d.%m.%Y').date()


def strs_to_datetime_departure(sdate, stime):
    return datetime.strptime(sdate + '-' + stime, '%d.%m.%Y-%H:%M')


# Assuming that the date is the date of departure, not the date of arrival
def strs_to_datetime_arrival(sdate, stime, time_departure):
    time_arrival = datetime.strptime(sdate + '-' + stime, '%d.%m.%Y-%H:%M')
    if time_arrival < time_departure:
        return time_arrival + timedelta(days=1)
    else:
        return time_arrival


def format_datetimes(df):
    df.loc[:, 'departure'] = \
            df.loc[:, ['date', 'departure']].apply(
                    lambda c: strs_to_datetime_departure(
                        c['date'],
                        c['departure']),
                    axis=1)
    df.loc[:, 'arrival'] = \
            df.loc[:, ['date', 'arrival', 'departure']].apply(
                    lambda c: strs_to_datetime_arrival(
                        c['date'],
                        c['arrival'],
                        c['departure']),
                    axis=1)
    # date is redundant, but helpful for grouping.
    df.loc[:, 'date'] = df.loc[:, 'date'].apply(str_to_date)


def all_equal(lst):
    return len(set(lst)) == 1


def remove_unequal_delays(df):
    return df[df['delay'].apply(all_equal)]


def cancellation_to_int(s):
    if pd.isna(s):
        return 0
    elif s == 'Ausfall (Startbahnhof)':
        return 1
    else:
        return 0


def cancellation_to_int_lst(l):
    return [cancellation_to_int(c) for c in l]


data_in = pd.read_csv("data/scraped_incoming_Frankfurt_Hbf.csv",
                      names=['origin', 'destination', 'date', 'departure',
                             'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv("data/scraped_outgoing_Frankfurt_Hbf.csv",
                       names=['origin', 'destination', 'date', 'departure',
                              'arrival', 'train', 'delay', 'cancellation'])

format_datetimes(data_in)
format_datetimes(data_out)

# Use groupby with agg to apply custom aggregation function
result_in = data_in.groupby(['train', 'date', 'arrival', 'destination'])[
        ['origin', 'departure', 'delay', 'cancellation']
        ].agg(list_agg).reset_index()
result_out = data_out.groupby(['train', 'date', 'departure', 'origin'])[
        ['destination', 'arrival', 'delay', 'cancellation']
        ].agg(list_agg).reset_index()

# Remove entries from the df that don't have the same delay
# for every incoming train per station, as there probably is
# something wrong with the data point.
in_clean_delays = remove_unequal_delays(result_in)
out_clean_delays = remove_unequal_delays(result_out)

# Collapse and/or clean up lists
in_clean_delays.loc[:, 'delay'] = in_clean_delays.loc[:, 'delay'] \
        .apply(lambda l: l[0])
in_clean_delays.loc[:, 'cancellation'] = \
        in_clean_delays.loc[:, 'cancellation'].apply(cancellation_to_int_lst)
in_clean_delays = in_clean_delays.infer_objects()

min_time_differences = result_in.groupby(['date', 'train']).apply(min_time_diff)
print(min(min_time_differences))

merged = pd.merge(result_in, result_out, on=['date', 'train'])
merged['arrival_x'] = pd.to_datetime(merged['arrival_x'],
                                     format='%H:%M:%S', errors='coerce')
merged['departure_y'] = pd.to_datetime(merged['departure_y'],
                                       format='%H:%M:%S', errors='coerce')
condition = (
    (merged['arrival_x'] <= merged['departure_y']) &
    (merged['arrival_x'] > merged['departure_y'] - timedelta(minutes=60))
)
result = merged[condition]
print(result)
# result.to_csv('result.csv', index=False)
