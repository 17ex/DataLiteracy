import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from pathlib import Path
from preprocessing_funs import *

INPUT_DIR = "../../dat/scraped/"
OUTPUT_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


data_in = pd.read_csv(INPUT_DIR + "scraped_incoming_Frankfurt_Hbf.csv",
                      names=['origin', 'destination', 'date', 'departure',
                             'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv(INPUT_DIR + "scraped_outgoing_Frankfurt_Hbf.csv",
                       names=['origin', 'destination', 'date', 'departure',
                              'arrival', 'train', 'delay', 'cancellation'])

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

# Remove entries from the df that don't have the same delay
# for every incoming train per station, as there probably is
# something wrong with the data point.
in_clean_delays = remove_unequal_delays(result_in)

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
        ((merged['arrival_in'] <= merged['departure_out']) &
         (merged['arrival_in'] > merged['departure_out'] - timedelta(minutes=60))
         )
        )
merged = merged[condition]

# TODO
# Somehow ~5000 incoming trains and ~1800 outgoing trains are missing after this.
# What happened to them?
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

incoming = add_directions(incoming, True)
outgoing = add_directions(outgoing, False)

incoming.to_pickle(OUTPUT_DIR + "incoming.pkl")
outgoing.to_pickle(OUTPUT_DIR + "outgoing.pkl")
