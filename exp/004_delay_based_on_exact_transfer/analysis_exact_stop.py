from collections import defaultdict

import pandas as pd
import json
import timeit
import pickle
import sys
from pathlib import Path

"""
This is an attempt to get the delay and reachable transfers for every pair of stations individually.
This solves the problem with cancellations that was tried to be solved with looking at directions.
With this attempt there comes the problem of compute time and therefore some compromises have been made:
1. We again have to merge the trains in Frankfurt by date so we loose some transfers around midnight every day.
2. Only transfers that are one hour apart are considered.
3. When a transfer is not reachable only transfers in the next 3 hours are looked at as an alternative.

All in all it still takes a very long time to compute all combinations of stations, but the results might be worth it."""


DATA_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)


def find_biggest_gain_per_next_stop(incoming, outgoing):
    """
    Finds the biggest gain per next stop based on incoming and outgoing train data.

    Args:
    - incoming (DataFrame): DataFrame containing incoming train information.
    - outgoing (DataFrame): DataFrame containing outgoing train information.

    Returns:
    - gains (dict): Dictionary containing the biggest gain per next stop.
    - average_gain (dict): Dictionary containing average gains per destination.
    """

    gains = {}  # Dictionary to store the biggest gain per next stop
    average_gain = {}  # Dictionary to store average gains per next stop
    merged = pd.merge(incoming, outgoing, on='in_id', how='inner')
    acc_too_large = 0
    acc_normal = 0
    acc_cancelled = 0

    for row in merged.itertuples():
        # Extracting relevant information from the merged DataFrame
        departure = row.departure_y
        arrival = row.arrival_x
        delay_in = row.delay_x
        delay_out = row.delay_y[0]
        destination = row.destination_y[0]

        # Skipping rows with cancellations, as they don't contain useful gain information
        if 1 in row.cancellation_x or 2 in row.cancellation_x or 1 in row.cancellation_y or 2 in row.cancellation_y:
            acc_cancelled += 1
            continue

        driving_time = (row.arrival_y[0] - departure).total_seconds() / 60
        wait_time = (departure - arrival).total_seconds() / 60
        departure_delay = max(0, delay_in - wait_time)
        gain = departure_delay - delay_out

        # Handling cases where gain exceeds a threshold of 10% of the time the train takes
        if gain > 0.1 * driving_time:
            delays = row.delay_y
            delay_out = -1
            # adjust for the errors in the data where large delays go to 0 and then back up to the actual delay
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
            average_gain[destination] = (t + 1, (t * v + gain) / (t + 1))

    return gains, average_gain


def find_next_train(train, filtered_next, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Finds the next train based on certain criteria.

    Args:
    - train (DataFrame): DataFrame containing information about the current train.
    - all_trains (DataFrame): DataFrame containing information about all trains.
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - next_train (Next Train in the Dataframe or None): The next train information, if found; otherwise, None.
    - time_difference (float): Time difference in minutes between the next train's departure and the current train's plan_departure.
    """
    start_time = timeit.default_timer()
    dest_idx = train.destination_idx
    plan_arrival = train.arrival_y[dest_idx]
    # only look at trains that arrive later in the destination
    filtered_next = filtered_next[
        filtered_next.apply(lambda row: row['arrival_y'][row['destination_idx']] > plan_arrival, axis=1)]
    while not filtered_next.empty:
        next_train_idx = filtered_next.apply(lambda row: row['arrival_y'][row['destination_idx']], axis=1).idxmin()
        next_train = filtered_next.loc[next_train_idx]
        dest_idx = next_train.destination_idx
        cancellation_out = next_train.cancellation_y
        plan_difference, delay_difference = reachable_train(next_train, gains, estimated_gain, worst_case)
        if plan_difference <= delay_difference or cancellation_out[dest_idx] != 0:
            filtered_next = filtered_next.drop(next_train_idx)
        else:
            del filtered_next
            return next_train, (next_train.arrival_y[dest_idx] - plan_arrival).total_seconds() / 60, dest_idx
    execution_time = timeit.default_timer() - start_time
    del filtered_next
    return None, 0, 0


def reachable_train(train, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Calculates the plan difference and delay difference for a given train.

    Args:
    - train (row of a DataFrame): DataFrame containing information about the train.
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - plan_difference (float): Time difference between departure and arrival at a specific station.
    - delay_difference (float): Difference between the planned delay and the actual delay.
    """
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
        gain = gains.get(destination, 0)
        out_delay = max(0, dest_delay[0] + gain)
        delay_difference = max(0, in_delay - out_delay)
    else:
        estimated_gain * (dest_arrival - departure_FRA).total_seconds() / 60
        out_delay = max(0, dest_delay[0] + estimated_gain)
        delay_difference = max(0, in_delay - out_delay)
    return plan_difference, delay_difference


def reachable_transfers(incoming_origin, outgoing, origin, destination, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Identifies reachable transfers between incoming and outgoing trains.

    Args:
    - incoming (DataFrame): DataFrame containing incoming train information.
    - outgoing (DataFrame): DataFrame containing outgoing train information.
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - reachable_count (dict): Count of reachable and unreachable transfers based on plan and delay differences.
    - delay (dict): Average delay information for each plan difference.
    """
    start_time = timeit.default_timer()
    max_hours = 3
    outgoing_dest = outgoing[outgoing['destination'].apply(lambda x: any(destination == value for value in x))]
    print('incoming: ', len(incoming_origin), 'outgoing: ', len(outgoing_dest))
    # filtered = incoming_origin.merge(outgoing_dest, how='cross')
    filtered = incoming_origin.merge(outgoing_dest, how='outer', on='date')
    filtered['time_difference'] = (filtered['departure_y'] - filtered['arrival_x']).dt.total_seconds() / 60
    filtered = filtered[(filtered['arrival_x'] < filtered['departure_y']) & (filtered['time_difference'] <= max_hours * 60)
                        & (filtered['in_id_x'] != filtered['in_id_y'])]
    filtered.loc[:, 'destination_idx'] = filtered['destination_y'].apply(lambda x: x.index(destination))
    print(len(filtered))
    execution_time = timeit.default_timer() - start_time
    print("filter time: ", round(execution_time, 4))
    reachable_count = {}
    delay = {}
    discarded = 0
    start_time_all = timeit.default_timer()
    unique_ids = filtered['in_id_x'].unique()
    found_train_x = 0
    not_found_x = 0
    found_train_y = 0
    not_found_y = 0
    for id in set(unique_ids):
        group_id = filtered[filtered['in_id_x'] == id]
        example_train = group_id.iloc[0]
        group_date = filtered[filtered['date'] == example_train['date']]
        for train in group_id.itertuples():
            if train.time_difference > 60:
                discarded += 1
                continue
            start_time = timeit.default_timer()
            origin_idx = int(train.origin_idx)
            dest_idx = train.destination_idx
            plan_arrival = train.arrival_y[dest_idx]
            plan_departure_FRA = train.departure_y
            plan_departure_origin = train.departure_origin
            plan_difference, delay_difference = reachable_train(train, gains, estimated_gain, worst_case)
            if plan_difference not in reachable_count:
                reachable_count[plan_difference] = [0, 0]
                delay[plan_difference] = []

            # case if the stops of the arriving train were cancelled
            if train.cancellation_x[origin_idx] != 0 or train.cancellation_x[-1] != 0:
                reachable_count[plan_difference][1] += 1
                # filtering so these trains have a planned departure at the origin after the original train
                filtered_next = group_date[group_date['departure_origin'] > plan_departure_origin]
                next_train, extra_delay, dest_idx = find_next_train(train, filtered_next, gains, estimated_gain, worst_case)
                if next_train is not None:
                    found_train_x += 1
                    delay[plan_difference].append(next_train.delay_y[dest_idx] + extra_delay)
                else:
                    not_found_x += 1
                    delay[plan_difference].append(max_hours * 60)
            # case if the stops of the leaving train were cancelled or transfer not possible
            elif train.cancellation_y[dest_idx] != 0 or plan_difference <= delay_difference:
                reachable_count[plan_difference][1] += 1
                # only looking at trains that leave later in Frankfurt
                filtered_next = group_id[group_id['departure_y'] > plan_departure_FRA]
                next_train, extra_delay, dest_idx = find_next_train(train, filtered_next, gains, estimated_gain, worst_case)
                if next_train is not None:
                    found_train_y += 1
                    delay[plan_difference].append(next_train.delay_y[dest_idx] + extra_delay)
                else:
                    not_found_y += 1
                    delay[plan_difference].append(max_hours * 60)
            # case if train was reachable
            else:
                reachable_count[plan_difference][0] += 1
                delay[plan_difference].append(train.delay_y[dest_idx])
            execution_time = timeit.default_timer() - start_time
            if execution_time > 1:
                print(execution_time)
                print(filtered.memory_usage(deep=True).sum(), filtered_next.memory_usage(deep=True).sum(), get_dict_memory_usage(delay))
    execution_time_all = timeit.default_timer() - start_time_all
    print("Everything: ", round(execution_time_all, 4))
    if len(filtered) > 0:
        print(discarded / len(filtered))
    if found_train_x > 0 and found_train_y > 0:
        print(found_train_x, not_found_x, found_train_y, not_found_y)
    return reachable_count, delay

def get_dict_memory_usage(dictionary):
    total_size = sys.getsizeof(dictionary)
    for key, value in dictionary.items():
        total_size += sys.getsizeof(key)
        total_size += sys.getsizeof(value)
        if isinstance(value, dict):
            total_size += get_dict_memory_usage(value)
    return total_size


def create_inner_dict_reach():
    return defaultdict(lambda: [0, 0])

def create_inner_dict_delay():
    return defaultdict(list)


with open(DATA_DIR + 'incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open(DATA_DIR + 'outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)
gains, average_gain = find_biggest_gain_per_next_stop(incoming, outgoing)
average_gain = {key: value[1] for key, value in average_gain.items()}
incoming['date'] = pd.to_datetime(incoming['date'])
outgoing['date'] = pd.to_datetime(outgoing['date'])
list_of_incomings = [incoming]
list_of_outgoings = [outgoing]
# list_of_incomings = [group for _, group in incoming.groupby(pd.Grouper(key='date', freq='M'))]
# list_of_outgoings = [group for _, group in outgoing.groupby(pd.Grouper(key='date', freq='M'))]
directions = {'South': ['Weinheim(Bergstr)Hbf', 'Bruchsal', 'Karlsruhe-Durlach', 'Günzburg', 'Bensheim', 'Mannheim Hbf', 'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                        'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf', 'Offenburg', 'Freiburg(Breisgau) Hbf'],
              'West': ['Hamm(Westf)Hbf', 'Aachen Hbf', 'Mönchengladbach Hbf', 'Siegburg/Bonn', 'Hagen Hbf', 'Duisburg Hbf', 'Recklinghausen Hbf', 'Andernach', 'Köln/Bonn Flughafen', 'Solingen Hbf', 'Oberhausen Hbf', 'Montabaur', 'Münster(Westf)Hbf', 'Bochum Hbf', 'Wuppertal Hbf', 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                       'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf', 'Gelsenkirchen Hbf', 'Essen Hbf'],
              'North': ['Kassel-Wilhelmshöhe', 'Lüneburg', 'Göttingen', 'Hannover Messe/Laatzen', 'Uelzen', 'Hannover Hbf', 'Celle', 'Hamburg Dammtor', 'Neumünster', 'Treysa', 'Marburg(Lahn)', 'Gießen', 'Friedberg(Hess)', 'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kiel Hbf'],
              'North East': ['Weißenfels', 'Wittenberge', 'Naumburg(Saale)Hbf', 'Stendal Hbf', 'Halle(Saale)Hbf', 'Bitterfeld', 'Berlin Ostbahnhof','Berlin Südkreuz', 'Dresden-Neustadt', 'Wolfsburg Hbf', 'Eisenach', 'Dresden Hbf', 'Berlin-Spandau', 'Lutherstadt Wittenberg Hbf', 'Riesa', 'Hildesheim Hbf', 'Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf',
                             'Brandenburg Hbf', 'Magdeburg Hbf', 'Berlin Gesundbrunnen'],
              'East': ['München-Pasing', 'München Hbf', 'Augsburg Hbf', 'Plattling', 'Aschaffenburg Hbf', 'Passau Hbf', 'Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Ingolstadt Hbf']}

for i in range(len(list_of_incomings)):
    print(i)
    unique_values_in = set()
    unique_values_out = set()
    for sublist in list_of_incomings[i]['origin']:
        unique_values_in.update(sublist)
    for sublist in list_of_outgoings[i]['destination']:
        unique_values_out.update(sublist)
    k = 0
    for string in unique_values_in:
        if '/' in string:
            print(string)
    for origin in unique_values_in:
        if origin == 'Frankfurt(Main)Hbf':
            continue
        incoming_origin = incoming[incoming['origin'].apply(lambda x: any(origin == value for value in x))]
        incoming_origin['origin_idx'] = incoming_origin['origin'].apply(lambda x: x.index(origin))
        incoming_origin['departure_origin'] = incoming_origin.apply(lambda row: row['departure'][row['origin_idx']], axis=1)
        delay_all = defaultdict(create_inner_dict_delay)
        reachable_all = defaultdict(create_inner_dict_reach)
        print(origin, len(unique_values_in) - k)
        k += 1
        for destination in unique_values_out:
            print(destination)
            if destination == 'Frankfurt(Main)Hbf':
                continue
            if origin == destination:
                continue
            org_direction = None
            dest_direction = None
            for key, value_list in directions.items():
                if origin in value_list:
                    org_direction = key
                    break
            if org_direction is not None:
                for key, value_list in directions.items():
                    if destination in value_list:
                        dest_direction = key
                        break
            if dest_direction is not None:
                if org_direction and dest_direction and org_direction == dest_direction:
                    continue

            reachable_count_avg_gain, delay = reachable_transfers(incoming_origin, list_of_outgoings[i], origin, destination, gains=average_gain)
            for key in delay.keys():
                delay_all[destination][key].extend(delay[key])
            for key in reachable_count_avg_gain.keys():
                reachable_all[destination][key][0] += reachable_count_avg_gain[key][0]
                reachable_all[destination][key][1] += reachable_count_avg_gain[key][1]

        print(delay_all)
        print(reachable_all)
        if origin == 'Köln/Bonn Flughafen':
            origin = 'Köln-Bonn Flughafen'
        if origin == 'Hannover Messe/Laatzen':
            origin = 'Hannover Messe-Laatzen'
        if origin == 'Köln Messe/Deutz':
            origin = 'Köln Messe-Deutz'
        if origin == 'Siegburg/Bonn':
            origin = 'Siegburg-Bonn'
        with open(DATA_DIR + 'delay_per_stop/delay_{}.json'.format(origin), 'w') as file:
            json.dump(delay_all, file)
        with open(DATA_DIR + 'reachable_per_stop/reachable_{}.json'.format(origin), 'w') as file:
            json.dump(reachable_all, file)
