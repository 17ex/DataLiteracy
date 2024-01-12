import pandas as pd
import numpy as np
import json
import pickle
from pathlib import Path
from src.analysis_functions.exact_stop_functions import reachable_transfers
from src.analysis_functions.general_functions import find_gains_per_next_stop, get_directions


DATA_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
with open(DATA_DIR + 'incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open(DATA_DIR + 'outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

incoming['date'] = pd.to_datetime(incoming['date'])
outgoing['date'] = pd.to_datetime(outgoing['date'])

all_gains = find_gains_per_next_stop(incoming, outgoing)
median_gain = {}
average_gain = {}
max_gain = {}
for key in all_gains.keys():
    median_gain[key] = np.median(all_gains[key])
    average_gain[key] = np.mean(all_gains[key])
    max_gain[key] = np.amax(all_gains[key])

directions = get_directions()
unique_values_in = set()
unique_values_out = set()
for sublist in incoming['origin']:
    unique_values_in.update(sublist)
for sublist in outgoing['destination']:
    unique_values_out.update(sublist)
for origin in unique_values_in:
    if origin == 'Frankfurt(Main)Hbf':
        continue
    # do some pre-calculations for the incoming list
    incoming_origin = incoming[incoming['origin'].apply(lambda x: any(origin == value for value in x))]
    incoming_origin['origin_idx'] = incoming_origin['origin'].apply(lambda x: x.index(origin))
    incoming_origin['departure_origin'] = incoming_origin.apply(lambda row: row['departure'][row['origin_idx']], axis=1)
    delay_all = {}
    reachable_all = {}
    print(origin)
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

        reachable_count, delay = reachable_transfers(incoming_origin, outgoing, origin, destination, gains=average_gain)
        #TODO: speichern mit keys und nicht liste von listen. Und das hier eigentlich schon in reachable_transfer
        delay_all[destination] = delay
        reachable_all[destination] = reachable_count

    print(delay_all)
    print(reachable_all)
    #TODO: put this into preprocessing
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
