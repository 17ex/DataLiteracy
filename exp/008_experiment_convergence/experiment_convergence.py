import os
import sys
import pandas as pd
from parallel_pandas import ParallelPandas
REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                          os.pardir,
                                          os.pardir))
sys.path.insert(1, os.path.join(REPO_ROOT, 'src'))
from data_tools import format_station_name_file, load_excluded_pairs
import analysis
import data_io

ParallelPandas.initialize(n_cpu=4, split_factor=10, disable_pr_bar=False, show_vmem=True)
station_subset = data_io.load_station_subset()
incoming, outgoing = data_io.load_incoming_outgoing_conns()
excluded_pairs = load_excluded_pairs()
directions = data_io.load_directions()
unique_stations_in, unique_stations_out, _ = data_io.load_unique_station_names()

# This is here to silence the warnings regarding chained assignment
# that also display in the other experiments.
# The warning should be a false positive,
# and here it is silenced to allow for a progress bar to be shown.
pd.options.mode.chained_assignment = None


def get_direction(station):
    for key, value_list in directions.items():
        if station in value_list:
            return key
    return None


unique_stations_in = unique_stations_in.intersection(station_subset)
unique_stations_out = unique_stations_out.intersection(station_subset)
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


def calculate_delays_nowait_maxgain_per_origin(origin):
    incoming_from_origin = incoming[incoming['origin'].apply(
        lambda origin_list: origin in origin_list)]
    incoming_from_origin['origin_idx'] = incoming_from_origin['origin'].apply(
        lambda x: x.index(origin))
    incoming_from_origin['departure_origin'] = incoming_from_origin.apply(
        lambda row: row['departure'][row['origin_idx']], axis=1)
    incoming_from_origin['arrival_fra'] = incoming_from_origin['arrival'] \
        + pd.to_timedelta(incoming_from_origin['delay'], unit='m')
    delay_all_no_wait = {}
    delay_all_theoretical_max_gain = {}
    for destination in \
            station_pairs.loc[station_pairs['origin'] == origin, 'destination']:
        delay_no_wait = analysis.reachable_transfers(
                incoming_from_origin,
                outgoing,
                origin,
                destination,
                max_delay=180,
                max_hours=6,
                worst_case=True)
        delay_theoretical_max_gain = analysis.reachable_transfers(
                incoming_from_origin,
                outgoing,
                origin,
                destination,
                max_delay=180,
                max_hours=6,
                estimated_gain=0.27)
        delay_all_no_wait[destination] = delay_no_wait
        delay_all_theoretical_max_gain[destination] = delay_theoretical_max_gain
    data_io.write_json(delay_all_no_wait,
                       f'delay_008_{format_station_name_file(origin)}.json',
                       'results', 'no_wait'
                       )
    data_io.write_json(delay_all_theoretical_max_gain,
                       f'delay_008_{format_station_name_file(origin)}.json',
                       'results', 'theoretical_max_gain'
                       )


pd.Series(station_pairs['origin'].unique()) \
        .p_apply(calculate_delays_nowait_maxgain_per_origin)
