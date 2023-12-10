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

# Collapse and/or clean up lists
in_clean_delays.loc[:, 'delay'] = in_clean_delays.loc[:, 'delay'] \
        .apply(lambda l: l[0])
in_clean_delays.loc[:, 'cancellation'] = \
        in_clean_delays.loc[:, 'cancellation'].apply(cancellation_to_int_lst)
result_out.loc[:, 'cancellation'] = \
        result_out.loc[:, 'cancellation'].apply(cancellation_to_int_lst)
in_clean = in_clean_delays.infer_objects()
out_clean = result_out.infer_objects()

min_time_differences = in_clean.groupby(['date', 'train']).apply(min_time_diff)
print(min(min_time_differences))

len_in = len(in_clean)
len_out = len(out_clean)
in_clean['in_id'] = range(0, len_in)
out_clean['out_id'] = range(len_in, len_in + len_out)

merged = pd.merge(in_clean, out_clean, on=['date', 'train'], how='outer',
                  suffixes=['_in', '_out'])

# TODO
# The arrival station orders of out_clean are not sorted by time.
# Maybe sort them by time, or at least ensure that it is still consistent.
condition = (
        pd.isna(merged['arrival_in']) |
        pd.isna(merged['departure_out']) |
        ((merged['arrival_in'] <= merged['departure_out']) &
         (merged['arrival_in'] > merged['departure_out'] - timedelta(minutes=60))
         )
        )
merged = merged[condition]

incoming = merged.loc[merged.loc[:, 'in_id'].notna()]

# print(result)
# result.to_csv('result.csv', index=False)
