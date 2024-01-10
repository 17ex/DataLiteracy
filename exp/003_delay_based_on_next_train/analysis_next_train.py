import pandas as pd
import json
import numpy as np
import pickle
from pathlib import Path


DATA_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

# TODO: Update the directions and move this to preprocessing.
def add_direction(trains, is_incoming=False):
    """
    Adds direction information to train data based on destinations.

    Args:
    - trains (DataFrame): DataFrame containing train information with 'origin' and 'destination' columns.
    - is_incoming (bool, optional): Flag indicating whether to consider incoming trains. Default is False.

    Returns:
    None. Updates the 'direction' column in the trains DataFrame.

    The function populates the 'direction' column in the DataFrame based on the destinations of the trains.
    It checks the destinations against predefined directions and appends the corresponding direction to each train's row.
    If the destination doesn't match any predefined direction, it adds an empty string to the 'direction' column.
    """

    # Predefined directions and their associated destinations
    directions = {
        'South': ['Mannheim Hbf', 'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                  'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf', 'Augsburg Hbf'],
        'West': ['Frankfurt am Main Flughafen Fernbahnhof', 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                 'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf'],
        'North': ['Hannover Hbf', 'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kassel-Wilhelmshöhe', 'Kiel Hbf'],
        'North East': ['Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf', 'Brandenburg Hbf',
                       'Magdeburg Hbf', 'Berlin Gesundbrunnen', 'Bad Hersfeld', 'Fulda'],
        'East': ['Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Hanau Hbf']
    }

    # Initialize counters for found and not found directions
    found = 0
    not_found = 0
    direction_train = []
    for train in trains.itertuples():
        found_direction = False  # Flag to check if direction is found for the train's destination
        if is_incoming:
            destinations = train.origin  # Consider origin as destination for incoming trains
        else:
            destinations = train.destination

        # Checking the train's destination against predefined directions
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

        # If direction not found, add an empty string to the direction_train list
        if not found_direction:
            not_found += 1
            direction_train.append('')
    # Updating the DataFrame with the 'direction' column
    trains['direction'] = direction_train


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


def find_next_train_out(train, all_trains, gains={}, estimated_gain=0.0, worst_case=False):
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

    plan_departure = train.departure_y
    # only looking at trains in the same direction
    filtered_next = all_trains[(all_trains['departure_y'] > plan_departure) & (all_trains['direction_y'] == train.direction_y)].copy()

    while not filtered_next.empty:
        next_train_idx = filtered_next['time_difference'].idxmin()
        next_train = filtered_next.loc[next_train_idx]
        cancellation_in = next_train.cancellation_x
        cancellation_out = next_train.cancellation_y
        plan_difference, delay_difference = reachable_train(next_train, gains, estimated_gain, worst_case)
        # TODO: deal with cancellations in a better way
        if plan_difference <= delay_difference or cancellation_in[-1] != 0 or cancellation_out[0] != 0:
            filtered_next = filtered_next.drop(next_train_idx)
        else:
            return next_train, (next_train.departure_y - plan_departure).total_seconds() / 60

    return None, 0


def find_next_train_in(train, all_trains, gains={}, estimated_gain=0.0, worst_case=False):
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

    plan_arrival = train.arrival_x
    all_trains['arrival_diff'] = (all_trains['arrival_x'] - plan_arrival).dt.total_seconds() / 3600

    filtered_next = all_trains[(all_trains['arrival_x'] > plan_arrival) & (all_trains['arrival_diff'] < 6) & (all_trains['direction_x'] == train.direction_x)].copy()

    while not filtered_next.empty:
        next_train_idx = filtered_next['arrival_diff'].idxmin()
        next_train = filtered_next.loc[next_train_idx]
        cancellation_in = next_train.cancellation_x
        cancellation_out = next_train.cancellation_y
        plan_difference, delay_difference = reachable_train(train, gains, estimated_gain, worst_case)
        # TODO: deal with cancellations in a better way
        if plan_difference <= delay_difference or cancellation_in[-1] != 0 or cancellation_out[0] != 0:
            filtered_next = filtered_next.drop(next_train_idx)
        else:
            return train, max(0, (next_train.departure_y - train.departure_y).total_seconds() / 60)

    return None, 0


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


def reachable_transfers(incoming, outgoing, gains={}, estimated_gain=0.0, worst_case=False):
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

    filtered = incoming.merge(outgoing, how='cross')
    filtered['time_difference'] = (filtered['departure_y'] - filtered['arrival_x']).dt.total_seconds() / 3600
    filtered = filtered[(filtered['arrival_x'] < filtered['departure_y']) & (filtered['time_difference'] <= 12)
                        & (filtered['in_id_x'] != filtered['in_id_y']) & (filtered['direction_x'] != '') & (filtered['direction_y'] != '')]

    reachable_count = {}
    delay = {}
    unique_ids = filtered['in_id_x'].unique()
    no_next_train = 0
    found_train = 0

    for id in set(unique_ids):
        group_id = filtered[filtered['in_id_x'] == id]
        for train in group_id.itertuples():
            cancellation_in = train.cancellation_x
            cancellation_out = train.cancellation_y
            cancelled_ratio_in = cancellation_in.count(1) / len(cancellation_in)
            cancelled_ratio_out = cancellation_out.count(1) / len(cancellation_out)
            if cancellation_in[-1] == 1:
                cancelled_ratio_in = 1
            dest_delay = train.delay_y

            plan_difference, delay_difference = reachable_train(train, gains, estimated_gain, worst_case)

            if plan_difference not in reachable_count:
                reachable_count[plan_difference] = {'reachable': 0, 'not_reachable': 0}
                delay[plan_difference] = (0, 0)

            if plan_difference <= delay_difference:
                reachable_count[plan_difference]['not_reachable'] += 1
                next_train, wait_delay = find_next_train_out(train, group_id, gains, estimated_gain, worst_case)
                if next_train is not None:
                    t = delay[plan_difference][0]
                    v = delay[plan_difference][1]
                    delay[plan_difference] = (t + 1, (t * v + np.mean(next_train.delay_y) + wait_delay) / (t + 1))
                    found_train += 1
                else:
                    # TODO: How to deal with this (luckily doesn't happen super often)
                    no_next_train += 1
            elif cancelled_ratio_in > 0:
                # TODO: How to deal with incoming cancellations
                next_train_in, arrival_delay = find_next_train_in(train, filtered, gains, estimated_gain, worst_case)
                print(arrival_delay)
                reachable_count[plan_difference]['reachable'] += 1 - cancelled_ratio_in
                reachable_count[plan_difference]['not_reachable'] += cancelled_ratio_in
                t = delay[plan_difference][0]
                v = delay[plan_difference][1]
                if next_train_in is not None:
                    delay[plan_difference] = (t + 1, (
                                t * v + cancelled_ratio_in * (np.mean(next_train_in.delay_y) + arrival_delay) + (
                                    1 - cancelled_ratio_in) * np.mean(dest_delay)) / (t + 1))
                else:
                    no_next_train += 1
            elif cancelled_ratio_out > 0:
                reachable_count[plan_difference]['reachable'] += 1 - cancelled_ratio_out
                reachable_count[plan_difference]['not_reachable'] += cancelled_ratio_out
                t = delay[plan_difference][0]
                v = delay[plan_difference][1]
                next_train, wait_delay = find_next_train_out(train, group_id, gains, estimated_gain, worst_case)
                if next_train is not None:
                    delay[plan_difference] = (t + 1, (t * v + cancelled_ratio_out * (np.mean(next_train.delay_y) + wait_delay) + (1 - cancelled_ratio_out) * np.mean(dest_delay)) / (t + 1))
                else:
                    no_next_train += 1
            else:
                reachable_count[plan_difference]['reachable'] += 1
                t = delay[plan_difference][0]
                v = delay[plan_difference][1]
                delay[plan_difference] = (t + 1, (t * v + np.mean(dest_delay)) / (t + 1))

    return reachable_count, delay


with open(DATA_DIR + 'incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open(DATA_DIR + 'outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)
add_direction(incoming, is_incoming=True)
add_direction(outgoing, is_incoming=False)
gains, average_gain = find_biggest_gain_per_next_stop(incoming, outgoing)
average_gain = {key: value[1] for key, value in average_gain.items()}
incoming['date'] = pd.to_datetime(incoming['date'])
outgoing['date'] = pd.to_datetime(outgoing['date'])
list_of_incomings = [group for _, group in incoming.groupby(pd.Grouper(key='date', freq='M'))]
list_of_outgoings = [group for _, group in outgoing.groupby(pd.Grouper(key='date', freq='M'))]
for i in range(len(list_of_incomings)):
    print(i)
    reachable_count_avg_gain, delay = reachable_transfers(list_of_incomings[i], list_of_outgoings[i], gains=average_gain)
    with open(DATA_DIR + 'delay/delay_{}.json'.format(i), 'w') as file:
        json.dump(delay, file)

