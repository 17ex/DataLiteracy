import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path


DATA_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

with open(DATA_DIR + 'reachable/reach_worst_case.json', 'r') as file:
    reachable_worst_case = json.load(file)

with open(DATA_DIR + 'reachable/reach_0_gain.json', 'r') as file:
    reachable_count_0_gain = json.load(file)

with open(DATA_DIR + 'reachable/reach_max_gain.json', 'r') as file:
    reachable_count_max_gain = json.load(file)

with open(DATA_DIR + 'reachable/reach_avg_gain.json', 'r') as file:
    reachable_count_avg_gain = json.load(file)

with open(DATA_DIR + 'reachable/reach_02_gain.json') as file:
    reachable_count_02_gain = json.load(file)

print(reachable_worst_case)
print(reachable_count_0_gain)
print(reachable_count_max_gain)
print(reachable_count_avg_gain)

dicts = [reachable_count_0_gain]

reachable_percentage = {}

# Extract and calculate the percentage of reachable values for keys <= 60 from all dictionaries
for d in dicts:
    for key, value in d.items():
        if float(key) <= 30:
            if key not in reachable_percentage:
                reachable_percentage[key] = []
            reachable = value['reachable']
            not_reachable = value['not_reachable']
            total = reachable + not_reachable
            percentage = (reachable / total) * 100 if total > 0 else 0
            reachable_percentage[key].append(percentage)

# Sort keys numerically and align values for plotting
keys = sorted(reachable_percentage, key=lambda x: float(x))
max_len = max(len(reachable_percentage[key]) for key in keys)
values = np.zeros((len(keys), max_len))

for i, key in enumerate(keys):
    values[i, :len(reachable_percentage[key])] = reachable_percentage[key]

# Plotting the percentage of reachable values
plt.figure(figsize=(10, 6))

for i in range(max_len):
    plt.bar(keys, values[:, i], width=0.2, align='center', label=f'Dict {i + 1}', alpha=0.7)

plt.xlabel('Keys')
plt.ylabel('Percentage of Reachable Values')
plt.title('Percentage of Reachable Values for Keys <= 30')
plt.legend()
plt.xticks(keys)
plt.grid(axis='y')
plt.ylim(0, 100)  # Set y-axis limit from 0 to 100 (percentage range)

plt.show()
