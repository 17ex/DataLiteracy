import os
import sys
import pandas as pd
from parallel_pandas import ParallelPandas
REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                          os.pardir,
                                          os.pardir))
sys.path.insert(1, os.path.join(REPO_ROOT, 'src'))
import analysis
import data_io

ParallelPandas.initialize(n_cpu=6, split_factor=3, disable_pr_bar=False, show_vmem=True)
USE_SUBSET = False
station_subset = data_io.load_station_subset()
incoming, outgoing = data_io.load_incoming_outgoing_conns()
excluded_pairs = data_io.load_excluded_pairs()
gain_vals = data_io.load_gain_values('average')
directions = data_io.load_directions()
unique_stations_in, unique_stations_out, _ = data_io.load_unique_station_names()

# This is here to silence the warnings regarding chained assignment
# that also display in the other experiments.
# The warning should be a false positive,
# and here it is silenced to allow for a progress bar to be shown.
pd.options.mode.chained_assignment = None

# Drop data we don't need
del outgoing['origin']

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
        delay_all[destination] = analysis.reachable_transfers(
               incoming_from_origin,
               outgoing,
               origin,
               destination,
               gains=gain_vals)
    data_io.write_json(delay_all,
                       f'delay_007_{data_io.filename_escape(origin)}.json',
                       'results', 'exp_007', 'delay')
    return None


pd.Series(station_pairs['origin'].unique()).p_apply(calculate_delays_per_origin)
