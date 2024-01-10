import pandas as pd
import json
import pickle
from pathlib import Path


DATA_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def bootstrap(data, num_samples):
    resampled_data = []
    for _ in range(num_samples):
        resampled = data.sample(n=len(data), replace=True)
        resampled_data.append(resampled)
    return resampled_data


def find_biggest_gain_per_next_stop(incoming, outgoing):
    gains = {}
    average_gain = {}
    merged = pd.merge(incoming, outgoing, on='in_id', how='inner')
    acc_too_large = 0
    acc_normal = 0
    acc_cancelled = 0
    for row in merged.itertuples():
        departure = row.departure_y
        arrival = row.arrival_x
        delay_in = row.delay_x
        delay_out = row.delay_y[0]
        destination = row.destination_y[0]
        if 1 in row.cancellation_x or 2 in row.cancellation_x or 1 in row.cancellation_y or 2 in row.cancellation_y:
            acc_cancelled += 1
            continue
        driving_time = (row.arrival_y[0] - departure).total_seconds() / 60
        wait_time = (departure - arrival).total_seconds() / 60
        departure_delay = max(0, delay_in - wait_time)
        gain = departure_delay - delay_out
        if gain > 0.1 * driving_time:
            delays = row.delay_y
            delay_out = -1
            for j in range(len(delays)):
                if delays[j] != 0:
                    delay_out = delays[j]
                    destination = row.destination_y[j]
                    break
            if delay_out != -1:
                gain = departure_delay - delay_out
            else:
                gain = 0
            # print(gain, destination)
            acc_too_large += 1
            continue
        else:
            acc_normal += 1
        if destination not in gains.keys():
            gains[destination] = max(0, gain)
            average_gain[destination] = (1, gain)
        else:
            gains[destination] = max(gains[destination], gain)
            t = average_gain[destination][0]
            v = average_gain[destination][1]
            average_gain[destination] = (t + 1, (t * v + gain) / (t+1))
    print(acc_normal)
    print('gain more than 10 percent: ', acc_too_large)
    print(acc_cancelled)
    return gains, average_gain


def reachable_transfers(incoming, outgoing, gains={}, estimated_gain=0.0, worst_case=False):
    filtered = pd.merge(incoming, outgoing, on='date')
    filtered = filtered[(filtered['arrival_x'] < filtered['departure_y']) & (filtered['in_id_x'] != filtered['in_id_y'])]
    reachable_count = {}
    for train in filtered.itertuples():
        arrival_FRA = train.arrival_x
        departure_FRA = train.departure_y
        in_delay = train.delay_x
        dest_delay = train.delay_y[0]
        dest_arrival = train.arrival_y[0]
        destination = train.destination_y[0]
        cancellation_in = train.cancellation_x
        cancellation_out = train.cancellation_y
        plan_difference = (departure_FRA - arrival_FRA).total_seconds() / 60
        if worst_case:
            delay_difference = in_delay
        elif gains:
            if destination not in gains.keys():
                gain = 0
            else:
                gain = gains[destination]
            out_delay = max(0, dest_delay + gain)
            delay_difference = max(0, in_delay - out_delay)
        else:
            estimated_gain * (dest_arrival - departure_FRA).total_seconds() / 60
            out_delay = max(0, dest_delay + estimated_gain)
            delay_difference = max(0, in_delay - out_delay)

        if plan_difference not in reachable_count:
            reachable_count[plan_difference] = {'reachable': 0, 'not_reachable': 0}
        if plan_difference <= delay_difference or cancellation_in[-1] != 0 or cancellation_out[0] != 0:
            reachable_count[plan_difference]['not_reachable'] += 1
        else:
            reachable_count[plan_difference]['reachable'] += 1
    return reachable_count

with open(DATA_DIR + 'incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open(DATA_DIR + 'outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

num_bootstrap_samples = 0  # Number of bootstrap samples
incoming_bootstrapped = bootstrap(incoming, num_bootstrap_samples)
outgoing_bootstrapped = bootstrap(outgoing, num_bootstrap_samples)

bootstrapped_0_gain = []
bootstrapped_max_gain = []
bootstrapped_avg_gain = []
bootstrapped_worst_case = []
for i in range(num_bootstrap_samples):
    print(i)
    gains, average_gain = find_biggest_gain_per_next_stop(incoming_bootstrapped[i], outgoing_bootstrapped[i])
    average_gain = {key: value[1] for key, value in average_gain.items()}
    bootstrapped_0_gain.append(reachable_transfers(incoming_bootstrapped[i], outgoing_bootstrapped[i]))
    bootstrapped_max_gain.append(reachable_transfers(incoming_bootstrapped[i], outgoing_bootstrapped[i], gains=gains))
    bootstrapped_avg_gain.append(reachable_transfers(incoming_bootstrapped[i], outgoing_bootstrapped[i], gains=average_gain))
    bootstrapped_worst_case.append(reachable_transfers(incoming_bootstrapped[i], outgoing_bootstrapped[i], worst_case=True))

gains, average_gain = find_biggest_gain_per_next_stop(incoming, outgoing)
average_gain = {key: value[1] for key, value in average_gain.items()}

reachable_count_0_gain = reachable_transfers(incoming, outgoing)
reachable_count_max_gain = reachable_transfers(incoming, outgoing, gains=gains)
reachable_count_avg_gain = reachable_transfers(incoming, outgoing, gains=average_gain)
reachable_count_worst_case = reachable_transfers(incoming, outgoing, worst_case=True)

with open(DATA_DIR + 'reachable/reach_worst_case.json', 'w') as file:
    json.dump(reachable_count_worst_case, file)

with open(DATA_DIR + 'reachable/reach_0_gain.json', 'w') as file:
    json.dump(reachable_count_0_gain, file)

with open(DATA_DIR + 'reachable/reach_max_gain.json', 'w') as file:
    json.dump(reachable_count_max_gain, file)

with open(DATA_DIR + 'reachable/reach_avg_gain.json', 'w') as file:
    json.dump(reachable_count_avg_gain, file)

with open(DATA_DIR + 'reachable/bootstrap/reach_avg_gain.json', 'w') as file:
    json.dump(bootstrapped_avg_gain, file)

with open(DATA_DIR + 'reachable/bootstrap/reach_0_gain.json', 'w') as file:
    json.dump(bootstrapped_0_gain, file)

with open(DATA_DIR + 'reachable/bootstrap/reach_max_gain.json', 'w') as file:
    json.dump(bootstrapped_max_gain, file)

with open(DATA_DIR + 'reachable/bootstrap/reach_worst_case.json', 'w') as file:
    json.dump(bootstrapped_worst_case, file)
