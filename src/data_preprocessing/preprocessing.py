import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from pathlib import Path
from preprocessing_funs import *
import requests

DATA_DIR = "../../dat/"
INPUT_DIR = DATA_DIR + "scraped/"
OUTPUT_DIR = DATA_DIR + "train_data/frankfurt_hbf/"
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


data_in = pd.read_csv(INPUT_DIR + "scraped_incoming_Frankfurt_Hbf.csv",
                      names=['origin', 'destination', 'date', 'departure',
                             'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv(INPUT_DIR + "scraped_outgoing_Frankfurt_Hbf.csv",
                       names=['origin', 'destination', 'date', 'departure',
                              'arrival', 'train', 'delay', 'cancellation'])

print(f"Number of incoming datapoints: {len(data_in)}")
print(f"Number of outgoing datapoints: {len(data_out)}")

format_datetimes(data_in)
format_datetimes(data_out)

data_in = data_in.sort_values(["date", "departure"])
data_out = data_out.sort_values(["date", "arrival"])

# Use groupby with agg to apply custom aggregation function
result_in = data_in.groupby(['train', 'date', 'arrival', 'destination'])[
        ['origin', 'departure', 'delay', 'cancellation']
        ].agg(list_agg).reset_index()
result_out = data_out.groupby(['train', 'date', 'departure', 'origin'])[
        ['destination', 'arrival', 'delay', 'cancellation']
        ].agg(list_agg).reset_index()


# Interpolate delays for data points with 0 delay
# when it is definitely an error in the data.
changes = result_out.apply(fix_delays, axis=1)
result_out['delay'] = changes.apply(lambda x: x[0])
total_changes = changes.apply(lambda x: x[1]).sum()

# Count total entries
num_entries = 0
for delay_list in result_out["delay"]:
    num_entries += len(delay_list)

print(f"Set {total_changes} of {num_entries} wrong 0 delays to an interpolated value.")

# Remove entries from the df that don't have the same delay
# for every incoming train per station, as there probably is
# something wrong with the data point.
initial_incoming_length = len(result_in)
in_clean_delays = remove_unequal_delays(result_in)
print(f"Removed {initial_incoming_length - len(result_in)} incoming trains with varying delays.")

# Collapse and/or clean up lists
in_clean_delays.loc[:, 'delay'] = in_clean_delays.loc[:, 'delay'] \
        .apply(lambda l: l[0])
in_clean_delays.loc[:, 'cancellation'] = \
        in_clean_delays.loc[:, 'cancellation'].apply(cancellation_to_int_lst)
result_out.loc[:, 'cancellation'] = \
        result_out.loc[:, 'cancellation'].apply(cancellation_to_int_lst)
in_clean = in_clean_delays.infer_objects()
out_clean = result_out.infer_objects()

# min_time_differences = in_clean.groupby(['date', 'train']).apply(min_time_diff)
# print(min(min_time_differences))

len_in = len(in_clean)
len_out = len(out_clean)
in_clean['in_id'] = range(0, len_in)
out_clean['out_id'] = range(len_in, len_in + len_out)

merged = pd.merge(in_clean, out_clean, on=['date', 'train'], how='outer',
                  suffixes=['_in', '_out'])


condition = (
        pd.isna(merged['arrival_in']) |
        pd.isna(merged['departure_out']) |
        ((merged['arrival_in'] <= merged['departure_out']) &    # drop wrongly merged trains
         (merged['arrival_in'] > merged['departure_out'] - timedelta(minutes=60))
         )
        )
merged = merged[condition]

incoming = merged.loc[merged.loc[:, 'in_id'].notna()]
outgoing = merged.loc[merged.loc[:, 'out_id'].notna()]
incoming = incoming.loc[:, ['in_id', 'train', 'date', 'arrival_in',
                            'destination_in', 'origin_in', 'departure_in',
                            'delay_in', 'cancellation_in', 'out_id']]
outgoing = outgoing.loc[:, ['out_id', 'train', 'date', 'arrival_out',
                            'destination_out', 'origin_out', 'departure_out',
                            'delay_out', 'cancellation_out', 'in_id']]
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

incoming = add_directions(incoming, True, debug=False)
outgoing = add_directions(outgoing, False, debug=False)

incoming.to_pickle(OUTPUT_DIR + "incoming.pkl")
outgoing.to_pickle(OUTPUT_DIR + "outgoing.pkl")


# Download file containing train station coordinates
coordinates_file = Path(DATA_DIR + "coordinates.csv")
if not coordinates_file.is_file():
    coordinates_file_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    print(f"Downloading station files from: {coordinates_file_url}")
    resp = requests.get(coordinates_file_url)
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
exclusion_file = Path(DATA_DIR + "excluded_pairs.csv")
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
