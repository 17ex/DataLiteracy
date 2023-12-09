import pandas as pd
from datetime import timedelta
from datetime import datetime


def min_time_diff(group):
    min_diff = float('inf')  # Set an initial maximum value for minimum difference
    times = group['arrival'].tolist()

    for i in range(len(times)):
        for j in range(i + 1, len(times)):
            time_diff = abs((datetime.combine(datetime.today(), times[j]) - datetime.combine(datetime.today(),
                                                                                             times[i])).total_seconds())
            if min_diff > time_diff:
                min_diff = time_diff
    return min_diff


# Define a custom aggregation function to collect the values in to a list
def list_agg(x):
    return list(x)

data_in = pd.read_csv("data/scraped_incoming_Frankfurt_Hbf.csv",
                         names=['origin', 'destination', 'date', 'departure', 'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv("data/scraped_outgoing_Frankfurt_Hbf.csv",
                         names=['origin', 'destination', 'date', 'departure', 'arrival', 'train', 'delay', 'cancellation'])
# Use groupby with agg to apply custom aggregation function
data_in['date'] = data_in['date'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
data_in['departure'] = data_in['departure'].apply(lambda x: datetime.strptime(x, '%H:%M').time())
data_in['arrival'] = data_in['arrival'].apply(lambda x: datetime.strptime(x, '%H:%M').time())
data_out['date'] = data_out['date'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
data_out['departure'] = data_out['departure'].apply(lambda x: datetime.strptime(x, '%H:%M').time())
data_out['arrival'] = data_out['arrival'].apply(lambda x: datetime.strptime(x, '%H:%M').time())
def list_agg(x):
    return list(x)

# Use groupby with agg to apply custom aggregation function
result_in = data_in.groupby(['train', 'date', 'arrival', 'destination'])[['origin', 'departure', 'delay', 'cancellation']].agg(list_agg).reset_index()
result_out = data_out.groupby(['train', 'date', 'departure', 'origin'])[['destination', 'arrival', 'delay', 'cancellation']].agg(list_agg).reset_index()
min_time_differences = result_in.groupby(['date', 'train']).apply(min_time_diff)
print(min(min_time_differences))
merged = pd.merge(result_in, result_out, on=['date', 'train'])
merged['arrival_x'] = pd.to_datetime(merged['arrival_x'], format='%H:%M:%S', errors='coerce')
merged['departure_y'] = pd.to_datetime(merged['departure_y'], format='%H:%M:%S', errors='coerce')
condition = (
    (merged['arrival_x'] <= merged['departure_y']) &
    (merged['arrival_x'] > merged['departure_y'] - timedelta(minutes=60))
)
result = merged[condition]
print(result)
# result.to_csv('result.csv', index=False)


