import pandas as pd
import numpy as np
import json
import pickle
from pathlib import Path
import sys
import os

REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                          os.pardir,
                                          os.pardir))
sys.path.insert(1, os.path.join(REPO_ROOT, 'src'))
from data_tools import format_station_name_file, load_excluded_pairs
import general_functions as general
import exact_stop_functions as exact_stop
import data_io

station_subset_in = ['Essen Hbf', 'Leipzig Hbf', 'Magdeburg Hbf', 'Hamburg Hbf', 'Kiel Hbf', 'Stuttgart Hbf', 'Potsdam Hbf'
    , 'Berlin Hbf', 'Erfurt Hbf', 'Hannover Hbf', 'Köln Hbf', 'Schwerin Hbf', 'München Hbf', 'Düsseldorf Hbf'
    , 'Duisburg Hbf', 'Dresden Hbf', 'Mainz Hbf', 'Bremen Hbf', 'Saarbrücken Hbf', 'Dortmund Hbf', 'Karlsruhe Hbf'
    , 'Nürnberg Hbf', 'Wiesbaden Hbf', 'Köln Hbf']
station_subset_out = ['Essen Hbf', 'Leipzig Hbf', 'Magdeburg Hbf', 'Hamburg Hbf', 'Kiel Hbf', 'Stuttgart Hbf', 'Potsdam Hbf'
    , 'Berlin Hbf', 'Erfurt Hbf', 'Hannover Hbf', 'Köln Hbf', 'Schwerin Hbf', 'München Hbf', 'Düsseldorf Hbf'
    , 'Duisburg Hbf', 'Dresden Hbf', 'Mainz Hbf', 'Bremen Hbf', 'Saarbrücken Hbf', 'Dortmund Hbf', 'Karlsruhe Hbf'
    , 'Nürnberg Hbf', 'Wiesbaden Hbf', 'Köln Hbf']

incoming, outgoing = data_io.load_incoming_outgoing_conns()
excluded_pairs = load_excluded_pairs()

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
    if origin not in station_subset_in:
        continue
    # do some pre-calculations for the incoming list
    incoming_from_origin = incoming[incoming['origin'].apply(lambda x: any(origin == value for value in x))]
    incoming_from_origin['origin_idx'] = incoming_from_origin['origin'].apply(lambda x: x.index(origin))
    incoming_from_origin['departure_origin'] = incoming_from_origin.apply(lambda row: row['departure'][row['origin_idx']], axis=1)
    incoming_from_origin['arrival_fra'] = incoming_from_origin['arrival'] + pd.to_timedelta(incoming_from_origin['delay'], unit='m')
    delay_all_no_wait = {}
    delay_all_theoretical_max_gain = {}
    print(origin)
    for destination in unique_stations_out:
        if (
                destination not in station_subset_out
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
        delay_no_wait = exact_stop.reachable_transfers(incoming_from_origin, outgoing, origin, destination,
                                                       max_delay=180, max_hours=6, worst_case=True)
        delay_theoretical_max_gain = exact_stop.reachable_transfers(incoming_from_origin, outgoing, origin, destination,
                                                                    max_delay=180, max_hours=6, estimated_gain=0.27)
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
