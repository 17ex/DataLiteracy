import pandas as pd
import json
import numpy as np
import pickle


def add_direction(trains, is_incoming=False):
    directions = {'South': ['Mannheim Hbf', 'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                            'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf',
                            'Augsburg Hbf'],
                  'West': ['Frankfurt am Main Flughafen Fernbahnhof', 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                           'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf',
                           'Wiesbaden Hbf'],
                  'North': ['Hannover Hbf', 'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kassel-Wilhelmshöhe',
                            'Kiel Hbf'],
                  'North East': ['Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf',
                                 'Brandenburg Hbf', 'Magdeburg Hbf', 'Berlin Gesundbrunnen', 'Bad Hersfeld', 'Fulda'],
                  'East': ['Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Hanau Hbf']}
    found = 0
    not_found = 0
    direction_train = []
    for train in trains.itertuples():
        found_direction = False
        if is_incoming:
            destinations = train.origin
        else:
            destinations = train.destination
        for dest in destinations:
            if dest in directions['South']:
                found += 1
                found_direction = True
                direction_train.append('South')
                break
            elif dest in directions['West']:
                found += 1
                found_direction = True
                direction_train.append('West')
                break
            elif dest in directions['North']:
                found += 1
                found_direction = True
                direction_train.append('North')
                break
            elif dest in directions['North East']:
                found += 1
                found_direction = True
                direction_train.append('North East')
                break
            elif dest in directions['East']:
                found += 1
                found_direction = True
                direction_train.append('East')
                break
        if not found_direction:
            not_found += 1
            direction_train.append('')
    print(found, not_found)
    trains['direction'] = direction_train

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


def find_next_train(train, all_trains, gains={}, estimated_gain=0.0, worst_case=False):
    plan_departure = train.departure_y
    filtered = all_trains[(all_trains['departure_y'] > plan_departure) & (all_trains['direction_y'] == train.direction_y)]
    while not filtered.empty:
        next_train_idx = filtered['time_difference'].idxmin()
        next_train = filtered.loc[next_train_idx]
        cancellation_in = next_train.cancellation_x
        cancellation_out = next_train.cancellation_y
        plan_difference, delay_difference = reachable_train(next_train, gains, estimated_gain, worst_case)
        if plan_difference <= delay_difference or cancellation_in[-1] != 0 or cancellation_out[0] != 0:
            filtered = filtered.drop(next_train_idx)
        else:
            return next_train, (next_train.departure_y - plan_departure).total_seconds() / 60
    return None, 0


def reachable_train(train, gains={}, estimated_gain=0.0, worst_case=False):
    arrival_FRA = train.arrival_x
    departure_FRA = train.departure_y
    in_delay = train.delay_x
    dest_delay = train.delay_y
    dest_arrival = train.arrival_y[0]
    destination = train.destination_y[0]
    plan_difference = (departure_FRA - arrival_FRA).total_seconds() / 60
    if worst_case:
        delay_difference = in_delay
    elif gains:
        if destination not in gains.keys():
            gain = 0
        else:
            gain = gains[destination]
        out_delay = max(0, dest_delay[0] + gain)
        delay_difference = max(0, in_delay - out_delay)
    else:
        estimated_gain * (dest_arrival - departure_FRA).total_seconds() / 60
        out_delay = max(0, dest_delay[0] + estimated_gain)
        delay_difference = max(0, in_delay - out_delay)
    return plan_difference, delay_difference


def reachable_transfers(incoming, outgoing, gains={}, estimated_gain=0.0, worst_case=False):
    filtered = pd.merge(incoming, outgoing, on='date')
    filtered['time_difference'] = (filtered['departure_y'] - filtered['arrival_x']).dt.total_seconds() / 3600
    filtered = filtered[(filtered['arrival_x'] < filtered['departure_y']) & (filtered['time_difference'] <= 5)
                        & (filtered['in_id_x'] != filtered['in_id_y']) & (filtered['direction_x'] != '') & (filtered['direction_y'] != '')]
    reachable_count = {}
    delay = {}
    unique_ids = filtered['in_id_x'].unique()
    length = len(set(unique_ids))
    i = 0
    for id in set(unique_ids):
        if i % 1000 == 0:
            print(length - i)
        i += 1
        group_id = filtered[filtered['in_id_x'] == id]
        for train in group_id.itertuples():
            cancellation_in = train.cancellation_x
            cancellation_out = train.cancellation_y
            dest_delay = train.delay_y
            plan_difference, delay_difference = reachable_train(train, gains, estimated_gain, worst_case)
            if plan_difference not in reachable_count:
                reachable_count[plan_difference] = {'reachable': 0, 'not_reachable': 0}
                delay[plan_difference] = (0, 0)
            if plan_difference <= delay_difference or cancellation_in[-1] != 0 or cancellation_out[0] != 0:
                reachable_count[plan_difference]['not_reachable'] += 1
                next_train, wait_delay = find_next_train(train, group_id, gains, estimated_gain, worst_case)
                if next_train is not None:
                    t = delay[plan_difference][0]
                    v = delay[plan_difference][1]
                    delay[plan_difference] = (t + 1, (t * v + np.mean(next_train.delay_y) + wait_delay) / (t + 1))
            else:
                reachable_count[plan_difference]['reachable'] += 1
                t = delay[plan_difference][0]
                v = delay[plan_difference][1]
                delay[plan_difference] = (t + 1, (t * v + np.mean(dest_delay)) / (t+1))
    return reachable_count, delay

with open('data/incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open('data/outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

add_direction(incoming, is_incoming=True)
add_direction(outgoing, is_incoming=False)
gains, average_gain = find_biggest_gain_per_next_stop(incoming, outgoing)
average_gain = {key: value[1] for key, value in average_gain.items()}
incoming_2021 = incoming[incoming['date'].apply(lambda date: date.year == 2021)]
incoming_2022 = incoming[incoming['date'].apply(lambda date: date.year == 2022)]
incoming_2023 = incoming[incoming['date'].apply(lambda date: date.year == 2023)]
outgoing_2021 = outgoing[outgoing['date'].apply(lambda date: date.year == 2021)]
outgoing_2022 = outgoing[outgoing['date'].apply(lambda date: date.year == 2022)]
outgoing_2023 = outgoing[outgoing['date'].apply(lambda date: date.year == 2023)]
reachable_count_avg_gain_2021, delay_2021 = reachable_transfers(incoming_2021, outgoing_2021, gains=average_gain)
with open('data/delay_2021.json', 'w') as file:
    json.dump(delay_2021, file)
reachable_count_avg_gain_2022, delay_2022 = reachable_transfers(incoming_2022, outgoing_2022, gains=average_gain)
with open('data/delay_2022.json', 'w') as file:
    json.dump(delay_2022, file)
reachable_count_avg_gain_2023, delay_2023 = reachable_transfers(incoming_2023, outgoing_2023, gains=average_gain)
with open('data/delay_2023.json', 'w') as file:
    json.dump(delay_2023, file)
