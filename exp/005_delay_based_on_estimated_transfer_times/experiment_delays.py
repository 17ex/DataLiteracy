import pandas as pd
import numpy as np
import json
import pickle
from pathlib import Path
import sys
import os

sys.path.insert(1, os.path.realpath(os.path.join(os.path.dirname(__file__),
                                                 os.pardir, os.pardir, 'src')))
from data_preprocessing.preprocessing_funs import format_station_name_file, load_excluded_pairs
import analysis_functions.general_functions as general
import analysis_functions.exact_stop_functions as exact_stop

station_subset = ['Essen Hbf', 'Leipzig Hbf', 'Magdeburg Hbf', 'Hamburg Hbf', 'Kiel Hbf', 'Stuttgart Hbf', 'Potsdam Hbf'
    , 'Berlin Hbf', 'Erfurt Hbf', 'Hannover Hbf', 'Köln Hbf', 'Schwerin Hbf', 'München Hbf', 'Düsseldorf Hbf'
    , 'Duisburg Hbf', 'Dresden Hbf', 'Mainz Hbf', 'Bremen Hbf', 'Saarbrücken Hbf', 'Dortmund Hbf', 'Karlsruhe Hbf'
    , 'Nürnberg Hbf', 'Wiesbaden Hbf', 'Köln Hbf']

DATA_DIR = "../../dat/train_data/frankfurt_hbf/"
SAVE_DIR = "../../dat/results/delay/"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(SAVE_DIR).mkdir(parents=True, exist_ok=True)
with open(DATA_DIR + 'incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open(DATA_DIR + 'outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

excluded_pairs = load_excluded_pairs("../../dat/")

incoming['date'] = pd.to_datetime(incoming['date'])
outgoing['date'] = pd.to_datetime(outgoing['date'])
all_gains = general.find_gains_per_next_stop(incoming, outgoing)
median_gain = {}
average_gain = {}
max_gain = {}
pos_avg_gain = {}
for key in all_gains.keys():
    median_gain[key] = np.median(all_gains[key])
    average_gain[key] = np.mean(all_gains[key])
    max_gain[key] = np.amax(all_gains[key])
    positive_numbers = [num for num in all_gains[key] if num > 0]
    pos_avg_gain[key] = np.mean(positive_numbers)
    # TODO properly handle case when there are no direct connections
    # (when the above are empty or 0)

directions = general.get_directions()

unique_stations_in = set()
unique_stations_out = set()
for sublist in incoming['origin']:
    unique_stations_in.update(sublist)

for sublist in outgoing['destination']:
    unique_stations_out.update(sublist)

unique_stations_in.remove('Frankfurt(Main)Hbf')
unique_stations_out.remove('Frankfurt(Main)Hbf')

for origin in unique_stations_in:
    if origin not in station_subset:
        continue
    # do some pre-calculations for the incoming list
    incoming_from_origin = incoming[incoming['origin'].apply(lambda x: any(origin == value for value in x))]
    incoming_from_origin['origin_idx'] = incoming_from_origin['origin'].apply(lambda x: x.index(origin))
    incoming_from_origin['departure_origin'] = incoming_from_origin.apply(lambda row: row['departure'][row['origin_idx']], axis=1)
    incoming_from_origin['arrival_fra'] = incoming_from_origin['arrival'] + pd.to_timedelta(incoming_from_origin['delay'], unit='m')
    delay_all = {}
    reachable_all = {}
    print(origin)
    for destination in unique_stations_out:
        if (
                destination not in station_subset
                or origin == destination
                or (origin, destination) in excluded_pairs
           ):
            continue
        org_direction = None
        dest_direction = None
        for key, value_list in directions.items():
            if origin in value_list:
                org_direction = key
                break
        if org_direction:
            for key, value_list in directions.items():
                if destination in value_list:
                    dest_direction = key
                    break
            if dest_direction and org_direction == dest_direction:
                # Maybe skip also if directions could not be determined,
                # and log it. Should not get called for that case though.
                continue
        print(destination)
        delay = exact_stop.reachable_transfers(incoming_from_origin, outgoing, origin, destination, gains=average_gain)
        delay_all[destination] = delay
    with open(SAVE_DIR + f'delay_{format_station_name_file(origin)}.json', 'w') as file:
        json.dump(delay_all, file)
