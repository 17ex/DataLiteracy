import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from pathlib import Path

INPUT_DIR = "../../dat/scraped/"
OUTPUT_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

def min_time_diff(group):
    min_diff = float('inf')  # Set an initial maximum value for minimum difference
    times = group['arrival'].tolist()
    for i, time_i in enumerate(times):
        for j in range(i + 1, len(times)):
            time_diff = abs((times[j] - time_i).total_seconds())
            min_diff = min(min_diff, time_diff)
    return min_diff


# Define a custom aggregation function to collect the values in to a list
def list_agg(x):
    return list(x)


def str_to_date(s):
    return datetime.strptime(s, '%d.%m.%Y').date()


def strs_to_datetime_departure(sdate, stime):
    return datetime.strptime(sdate + '-' + stime, '%d.%m.%Y-%H:%M')


# Assuming that the date is the date of departure, not the date of arrival
def strs_to_datetime_arrival(sdate, stime, time_departure):
    time_arrival = datetime.strptime(sdate + '-' + stime, '%d.%m.%Y-%H:%M')
    if time_arrival < time_departure:
        return time_arrival + timedelta(days=1)
    else:
        return time_arrival


def format_datetimes(df):
    df.loc[:, 'departure'] = \
            df.loc[:, ['date', 'departure']].apply(
                    lambda c: strs_to_datetime_departure(
                        c['date'],
                        c['departure']),
                    axis=1)
    df.loc[:, 'arrival'] = \
            df.loc[:, ['date', 'arrival', 'departure']].apply(
                    lambda c: strs_to_datetime_arrival(
                        c['date'],
                        c['arrival'],
                        c['departure']),
                    axis=1)
    # We use the date of a train as the date of departure.
    # If the train arrives a day later, it still has the date of departure
    # associated with it.
    df.loc[:, 'date'] = df.loc[:, 'departure'].apply(lambda d: d.date())


def all_equal(lst):
    return len(set(lst)) == 1


def remove_unequal_delays(df):
    return df[df['delay'].apply(all_equal)]


def cancellation_to_int(s):
    if pd.isna(s):
        return 0
    elif s == 'Ausfall (Startbahnhof)':
        return 1
    else:
        return 0


def cancellation_to_int_lst(l):
    return [cancellation_to_int(c) for c in l]


def d_id_to_int(d):
    if pd.isna(d):
        return -1
    else:
        return int(d)


def add_directions(train_data, is_incoming):

    direction_list = [""] * len(train_data)
    directions = {'South': ['Weinheim(Bergstr)Hbf', 'Bruchsal', 'Karlsruhe-Durlach', 'Günzburg', 'Bensheim', 'Mannheim Hbf', 'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                        'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf', 'Offenburg', 'Freiburg(Breisgau) Hbf'],
              'West': ['Hamm(Westf)Hbf', 'Aachen Hbf', 'Mönchengladbach Hbf', 'Siegburg/Bonn', 'Hagen Hbf', 'Duisburg Hbf', 'Recklinghausen Hbf', 'Andernach', 'Köln/Bonn Flughafen', 'Solingen Hbf', 'Oberhausen Hbf', 'Montabaur', 'Münster(Westf)Hbf', 'Bochum Hbf', 'Wuppertal Hbf', 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                       'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf', 'Gelsenkirchen Hbf', 'Essen Hbf'],
              'North': ['Kassel-Wilhelmshöhe', 'Lüneburg', 'Göttingen', 'Hannover Messe/Laatzen', 'Uelzen', 'Hannover Hbf', 'Celle', 'Hamburg Dammtor', 'Neumünster', 'Treysa', 'Marburg(Lahn)', 'Gießen', 'Friedberg(Hess)', 'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kiel Hbf'],
              'North East': ['Weißenfels', 'Wittenberge', 'Naumburg(Saale)Hbf', 'Stendal Hbf', 'Halle(Saale)Hbf', 'Bitterfeld', 'Berlin Ostbahnhof','Berlin Südkreuz', 'Dresden-Neustadt', 'Wolfsburg Hbf', 'Eisenach', 'Dresden Hbf', 'Berlin-Spandau', 'Lutherstadt Wittenberg Hbf', 'Riesa', 'Hildesheim Hbf', 'Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf',
                             'Brandenburg Hbf', 'Magdeburg Hbf', 'Berlin Gesundbrunnen'],
              'East': ['München-Pasing', 'München Hbf', 'Augsburg Hbf', 'Plattling', 'Aschaffenburg Hbf', 'Passau Hbf', 'Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Ingolstadt Hbf']}
    not_found = 0
    found = 0
    airport = 0
    index = 0
    remove_impossible_indices = -1

    for train_out in train_data.itertuples():
        found_direction = False

        if is_incoming:
            stops = train_out.origin
            stops.reverse()
        else:
            stops = train_out.destination

        # TODO check for impossible schedules in general? How?
        if 'Stuttgart Hbf' in stops and 'Berlin Hbf' in stops:
            remove_impossible_indices = index

        for dest in stops:
            if dest in directions['South']:
                found += 1
                found_direction = True
                direction_list[index] = 'South'
                break
            elif dest in directions['West']:
                found += 1
                found_direction = True
                direction_list[index] = 'West'
                break
            elif dest in directions['North']:
                found += 1
                found_direction = True
                direction_list[index] = 'North'
                break
            elif dest in directions['North East']:
                found += 1
                found_direction = True
                direction_list[index] = 'North East'
                break
            elif dest in directions['East']:
                found += 1
                found_direction = True
                direction_list[index] = 'East'
                break
        if not found_direction:
            not_found += 1
            direction_list[index] = 'None'
            for dest in train_out.destination:
                if dest in ['Frankfurt am Main Flughafen Fernbahnhof']:
                    airport += 1
                    break
        index += 1

    train_data['direction'] = direction_list

    print(f"Set directions of {found} trains.")
    print(f"Did not find clear direction of {not_found} trains.")
    if is_incoming:
        print(f"Out of those, {airport} trains start at Frankfurt airport without other stops.")
    else:
        print(f"Out of those, {airport} trains end at Frankfurt airport without other stops.")
    if remove_impossible_indices >= 0:
        train_data = train_data[train_data.index != remove_impossible_indices]
    return train_data

data_in = pd.read_csv(INPUT_DIR + "scraped_incoming_Frankfurt_Hbf.csv",
                      names=['origin', 'destination', 'date', 'departure',
                             'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv(INPUT_DIR + "scraped_outgoing_Frankfurt_Hbf.csv",
                       names=['origin', 'destination', 'date', 'departure',
                              'arrival', 'train', 'delay', 'cancellation'])

#add_directions(data_out)

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
