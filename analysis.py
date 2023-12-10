import pandas as pd
import pickle

with open('data/incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open('data/outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

pd.set_option('display.max_columns', None)
print(incoming)
print(outgoing)

# Get unique dates from both DataFrames
unique_dates_in = incoming['date'].unique()
unique_dates_out = outgoing['date'].unique()

reachable_count = {}

# Iterate over common dates in both DataFrames
for date in set(unique_dates_in) & set(unique_dates_out):
    group_in = incoming[incoming['date'] == date]
    group_out = outgoing[outgoing['date'] == date]
    for row_in in group_in.itertuples():
        for row_out in group_out.itertuples():
            if row_in.arrival <= row_out.departure or row_in.in_id == row_out.out_id:
                break
            else:
                plan_difference = (row_in.arrival - row_out.departure).total_seconds() / 60
                delay_difference = row_in.delay - row_out.delay[0]
                if plan_difference not in reachable_count:
                    reachable_count[plan_difference] = {'reachable': 0, 'not_reachable': 0}
                if plan_difference <= delay_difference:
                    reachable_count[plan_difference]['not_reachable'] += 1
                else:
                    reachable_count[plan_difference]['reachable'] += 1

# Sorting the dictionary by keys and creating a sorted list of tuples
sorted_items = sorted(reachable_count.items())

# Displaying the sorted list of tuples
for key, value in sorted_items:
    print(f"Key: {key}, Value: {value}")
