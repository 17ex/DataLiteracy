import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt

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
                # TODO: add logic for cancelled trains
                plan_difference = (row_in.arrival - row_out.departure).total_seconds() / 60
                delay_difference = row_in.delay - row_out.delay[0]
                if plan_difference not in reachable_count:
                    reachable_count[plan_difference] = {'reachable': 0, 'not_reachable': 0}
                if plan_difference <= delay_difference or row_in.cancellation[-1] != 0 or row_out.cancellation[0] != 0:
                    reachable_count[plan_difference]['not_reachable'] += 1
                else:
                    reachable_count[plan_difference]['reachable'] += 1

# Sorting the dictionary by keys and creating a sorted list of tuples
sorted_items = sorted(reachable_count.items())

# Displaying the sorted list of tuples
for key, value in sorted_items:
    print(f"Key: {key}, Value: {value}")

# Filter the data for keys smaller than 100 and sort by keys
filtered_data = [(key, value) for key, value in sorted_items if key < 100]
filtered_data.sort(key=lambda x: x[0])  # Sort by keys

# Calculate percentages for 'reachable' and 'not_reachable'
percentages_reachable = [(key, (value['reachable'] / (value['reachable'] + value['not_reachable'])) * 100) for key, value in filtered_data]
percentages_not_reachable = [(key, (value['not_reachable'] / (value['reachable'] + value['not_reachable'])) * 100) for key, value in filtered_data]

# Create bins and calculate average percentages for each bin
num_bins = int(max([key for key, _ in filtered_data]) / 5) + 1
bins = [[] for _ in range(num_bins)]

for i, (key, percentage) in enumerate(percentages_reachable):
    bin_index = int(key / 5)
    bins[bin_index].append(percentage)

average_reachable_percentages = [np.mean(bin_values) for bin_values in bins if bin_values]

# Create labels for bins
bin_labels = [f"{5 * i}-{5 * (i + 1) - 1}" for i in range(num_bins) if bins[i]]

# Plotting the average percentages per bin
plt.figure(figsize=(10, 6))

plt.bar(bin_labels, average_reachable_percentages, color='blue')
plt.xlabel('Planned transfer time')
plt.ylabel('Percentage of reachable transfers')
plt.title('Percentage of reachable transfers given the planned transfer time')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
