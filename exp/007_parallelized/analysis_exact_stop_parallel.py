import pandas as pd
import numpy as np
import json
import pickle
from pathlib import Path
from parallel_pandas import ParallelPandas
import sys

if Path.cwd().stem == '007_parallelized':
    sys.path.append('../..')

from src.data_preprocessing.preprocessing_funs import format_station_name_file, load_excluded_pairs
import src.analysis_functions.general_functions as general
import src.analysis_functions.exact_stop_functions as exact_stop

ParallelPandas.initialize(n_cpu=6, split_factor=3, disable_pr_bar=False, show_vmem=True)
USE_SUBSET = False
station_subset = {'Essen Hbf', 'Leipzig Hbf', 'Magdeburg Hbf', 'Hamburg Hbf', 'Kiel Hbf', 'Stuttgart Hbf', 'Potsdam Hbf'
    , 'Berlin Hbf', 'Erfurt Hbf', 'Hannover Hbf', 'Köln Hbf', 'Schwerin Hbf', 'München Hbf', 'Düsseldorf Hbf'
    , 'Duisburg Hbf', 'Dresden Hbf', 'Mainz Hbf', 'Bremen Hbf', 'Saarbrücken Hbf', 'Dortmund Hbf', 'Karlsruhe Hbf'
    , 'Nürnberg Hbf', 'Wiesbaden Hbf', 'Köln Hbf'}

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

# Drop data we don't need
del incoming['destination']
del outgoing['origin']


for sublist in incoming['origin']:
    unique_stations_in.update(sublist)

for sublist in outgoing['destination']:
    unique_stations_out.update(sublist)

unique_stations_in.remove('Frankfurt(Main)Hbf')
unique_stations_out.remove('Frankfurt(Main)Hbf')

if USE_SUBSET:
    unique_stations_in = unique_stations_in.difference(station_subset)
    unique_stations_out = unique_stations_out.difference(station_subset)


def get_direction(station):
    for key, value_list in directions.items():
        if station in value_list:
            return key
    return None


stations_in = pd.DataFrame(unique_stations_in, columns=['origin'])
stations_in['origin-dir'] = stations_in['origin'].apply(get_direction)
stations_out = pd.DataFrame(unique_stations_out, columns=['destination'])
stations_out['dest-dir'] = stations_out['destination'].apply(get_direction)
station_pairs = stations_in.join(stations_out, how='cross')
station_pairs = station_pairs[
    (station_pairs['origin'] != station_pairs['destination'])
    & station_pairs.apply(
        lambda pair: (pair['origin'], pair['destination']) not in excluded_pairs,
        axis=1)
    & (pd.isna(station_pairs['origin-dir'])
       | (station_pairs['origin-dir'] != station_pairs['dest-dir']))
    ]
del station_pairs['origin-dir']
del station_pairs['dest-dir']


def calculate_delays_per_origin(origin):
    incoming_from_origin = incoming[incoming['origin'].apply(
        lambda origin_list: origin in origin_list)]
    incoming_from_origin['origin_idx'] = incoming_from_origin['origin'].apply(
        lambda x: x.index(origin))
    incoming_from_origin['departure_origin'] = incoming_from_origin.apply(
        lambda row: row['departure'][row['origin_idx']], axis=1)
    incoming_from_origin['arrival_fra'] = incoming_from_origin['arrival'] \
        + pd.to_timedelta(incoming_from_origin['delay'], unit='m')
    delay_all = {}
    for destination in \
            station_pairs.loc[station_pairs['origin'] == origin, 'destination']:
        delay_all[destination] = exact_stop.reachable_transfers(
               incoming_from_origin,
               outgoing,
               origin,
               destination,
               gains=average_gain)
    with open(SAVE_DIR + f'delay_{format_station_name_file(origin)}.json', 'w') as file:
        json.dump(delay_all, file)
    return None


pd.Series(station_pairs['origin'].unique()).p_apply(calculate_delays_per_origin)
