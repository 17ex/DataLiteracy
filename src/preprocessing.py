import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from pathlib import Path
from data_tools import *
import data_io
import requests
import os
import general_functions as general

REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), os.pardir))
DATA_DIR = os.path.join(REPO_ROOT, "dat")
INPUT_DIR = os.path.join(DATA_DIR, "raw")
OUTPUT_DIR = os.path.join(DATA_DIR, "train_data", "frankfurt_hbf")
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

data_in = pd.read_csv(os.path.join(INPUT_DIR,
                                   "scraped_incoming_Frankfurt_Hbf.csv"),
                      names=['origin', 'destination', 'date', 'departure',
                             'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv(os.path.join(INPUT_DIR,
                                    "scraped_outgoing_Frankfurt_Hbf.csv"),
                       names=['origin', 'destination', 'date', 'departure',
                              'arrival', 'train', 'delay', 'cancellation'])
data_in, data_out = fix_duplicate_frankfurt(data_in, data_out)

print(f"Number of incoming datapoints: {len(data_in)}")
print(f"Number of outgoing datapoints: {len(data_out)}")

delays_in = data_in[pd.isna(data_in["cancellation"])]
delays_out = data_out[pd.isna(data_out["cancellation"])]
all_delays = np.append(np.array(delays_in["delay"]), np.array(delays_out["delay"]))
print(f"Mean delay: {np.mean(all_delays)}")

print("Formatting dates and times")
format_datetimes(data_in)
format_datetimes(data_out)

data_in = data_in.sort_values(["date", "departure"])
data_out = data_out.sort_values(["date", "arrival"])

# Merge individual rows (stops of a train) together into one train line,
# with lists specifying the stops, delays, etc.
print("Group data by trains")
result_in = data_in.groupby(['train', 'date', 'arrival', 'destination'])[
        ['origin', 'departure', 'delay', 'cancellation']
        ].agg(list).reset_index()
result_out = data_out.groupby(['train', 'date', 'departure', 'origin'])[
        ['destination', 'arrival', 'delay', 'cancellation']
        ].agg(list).reset_index()


# Collapse and/or clean up lists
pd.options.mode.chained_assignment = None
result_in.loc[:, 'delay'] = result_in.loc[:, 'delay'] \
        .apply(lambda l: l[0])
result_in.loc[:, 'cancellation'] = \
        result_in.loc[:, 'cancellation'].apply(cancellation_to_int_lst)
result_out.loc[:, 'cancellation'] = \
        result_out.loc[:, 'cancellation'].apply(cancellation_to_int_lst)
pd.options.mode.chained_assignment = 'warn'
in_clean = result_in.infer_objects()
out_clean = result_out.infer_objects()

# Assign ids to every individual train
len_in = len(in_clean)
len_out = len(out_clean)
in_clean['in_id'] = range(0, len_in)
out_clean['out_id'] = range(len_in, len_in + len_out)

print("Matching corresponding incoming and outgoing connections")
merged = pd.merge(in_clean, out_clean, on=['date', 'train'], how='outer',
                  suffixes=['_in', '_out'])
correctly_matched = (
        pd.isna(merged['arrival_in']) |
        pd.isna(merged['departure_out']) |
        ((merged['arrival_in'] <= merged['departure_out']) &
         (merged['arrival_in'] > merged['departure_out'] - timedelta(minutes=60))
         # Here, it is assumed that trains with the same train name have at
         # least one hour in-between different trains with the same train name,
         # which was manually verified to be the case for our dataset.
         )
        )
merged = merged[correctly_matched]

incoming = merged.loc[merged.loc[:, 'in_id'].notna()]
outgoing = merged.loc[merged.loc[:, 'out_id'].notna()]
incoming = incoming.loc[:, ['in_id', 'train', 'date', 'arrival_in',
                            'destination_in', 'origin_in', 'departure_in',
                            'delay_in', 'cancellation_in', 'out_id']]
outgoing = outgoing.loc[:, ['out_id', 'train', 'date', 'arrival_out',
                            'destination_out', 'origin_out', 'departure_out',
                            'delay_out', 'cancellation_out', 'in_id']]
# Rename the columns again after the merge and set some column types
incoming = incoming.rename(columns={'arrival_in': 'arrival',
                                    'destination_in': 'destination',
                                    'origin_in': 'origin',
                                    'departure_in': 'departure',
                                    'delay_in': 'delay',
                                    'cancellation_in': 'cancellation'})
outgoing = outgoing.rename(columns={'arrival_out': 'arrival',
                                    'destination_out': 'destination',
                                    'origin_out': 'origin',
                                    'departure_out': 'departure',
                                    'delay_out': 'delay',
                                    'cancellation_out': 'cancellation'})
incoming['out_id'] = incoming.loc[:, 'out_id'].apply(d_id_to_int).astype(int)
incoming['in_id'] = incoming.loc[:, 'in_id'].apply(d_id_to_int).astype(int)
outgoing['out_id'] = outgoing.loc[:, 'out_id'].apply(d_id_to_int).astype(int)
outgoing['in_id'] = outgoing.loc[:, 'in_id'].apply(d_id_to_int).astype(int)

print(f"Removed {len_in - len(incoming)} wrongly merged incoming trains.")
print(f"Removed {len_out - len(outgoing)} wrongly merged incoming trains.")

incoming = determine_train_direction(incoming, True, debug=True)
outgoing = determine_train_direction(outgoing, False, debug=True)
incoming = remove_wrong_incoming_trains(incoming)
outgoing = remove_wrong_outgoing_trains(outgoing)

incoming['date'] = pd.to_datetime(incoming['date'])
outgoing['date'] = pd.to_datetime(outgoing['date'])

incoming.to_pickle(os.path.join(OUTPUT_DIR, "incoming.pkl"))
outgoing.to_pickle(os.path.join(OUTPUT_DIR, "outgoing.pkl"))


data_io.write_unique_station_names(incoming, outgoing)
data_io.write_gain_vals(general.find_gains_per_next_stop(incoming, outgoing))


# Download file containing train station coordinates
coordinates_file = Path(os.path.join(DATA_DIR, "coordinates.csv"))
if not coordinates_file.is_file():
    coordinates_file_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    print(f"Downloading station files from: {coordinates_file_url}")
    resp = requests.get(coordinates_file_url, timeout=10)
    if resp.ok:
        with open(coordinates_file, mode="wb") as f:
            f.write(resp.content)
    else:
        print("WARNING: Download of coordinates.csv failed!")
        print(f"Server replied status {resp.status_code}")
        exit(1)
else:
    print("Station coordinate file already exists, skipping.")


# Create list of excluded origin, destination pairs
# TODO Remove check for file existence, move to data_io
exclusion_file = Path(os.path.join(DATA_DIR, "excluded_pairs.csv"))
if not exclusion_file.is_file():
    print("Determine station pairs to exclude from the analysis")
    write_excluded_station_pairs(
            pd.read_csv(coordinates_file, sep=';',
                        usecols=['NAME', 'Laenge', 'Breite']),
            unique_station_names(data_in, data_out),
            exclusion_file
            )
else:
    print("Station pair exclusion file already exists, skipping.")
