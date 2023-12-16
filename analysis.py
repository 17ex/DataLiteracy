import pandas as pd
import json
import pickle

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
            # print(gain, destination, ' driving time ', driving_time, ' wait ', wait_time, ' delay_Fra ', delay_in, ' delay_after_Fra ', row.delay_y)
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
                if row_in.arrival <= row_out.departure or row_in.in_id == row_out.in_id:
                    break
                else:
                    plan_difference = (row_in.arrival - row_out.departure).total_seconds() / 60
                    if worst_case:
                        delay_difference = row_in.delay
                    elif gains:
                        if row_out.destination[0] not in gains.keys():
                            gain = 0
                        else:
                            gain = gains[row_out.destination[0]]
                        delay_difference = max(0, row_in.delay - row_out.delay[0] - gain)
                    else:
                        estimated_gain = estimated_gain * (row_out.arrival[0] - row_out.departure).total_seconds() / 60
                        delay_difference = max(0, row_in.delay - row_out.delay[0] - estimated_gain)
                    if plan_difference not in reachable_count:
                        reachable_count[plan_difference] = {'reachable': 0, 'not_reachable': 0}
                    if plan_difference <= delay_difference or row_in.cancellation[-1] != 0 or row_out.cancellation[
                        0] != 0:
                        reachable_count[plan_difference]['not_reachable'] += 1
                    else:
                        reachable_count[plan_difference]['reachable'] += 1
    return reachable_count


with open('data/incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open('data/outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

gains, average_gain = find_biggest_gain_per_next_stop(incoming, outgoing)
average_gain = {key: value[1] for key, value in average_gain.items()}

reachable_count_0_gain = reachable_transfers(incoming, outgoing)
reachable_count_02_gain = reachable_transfers(incoming, outgoing, estimated_gain=0.2)
reachable_count_max_gain = reachable_transfers(incoming, outgoing, gains=gains)
reachable_count_avg_gain = reachable_transfers(incoming, outgoing, gains=average_gain)
reachable_count_worst_case = reachable_transfers(incoming, outgoing, worst_case=True)

with open('data/reachable/reach_worst_case.json', 'w') as file:
    json.dump(reachable_count_worst_case, file)

with open('data/reachable/reach_0_gain.json', 'w') as file:
    json.dump(reachable_count_0_gain, file)

with open('data/reachable/reach_max_gain.json', 'w') as file:
    json.dump(reachable_count_max_gain, file)

with open('data/reachable/reach_avg_gain.json', 'w') as file:
    json.dump(reachable_count_avg_gain, file)

with open('data/reachable/reach_02_gain.json', 'w') as file:
    json.dump(reachable_count_02_gain, file)
