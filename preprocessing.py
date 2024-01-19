import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np


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


def fix_delays(row):
    change_count = 0
    delays = row['delay']
    old_delays = delays.copy()
    #departures = row['departure']
    arrivals = row['arrival']
    
    for i in range(1, len(delays)):
        # Calculate time difference in minutes
        time_diff_minutes = (arrivals[i] - arrivals[i-1]).total_seconds() / 60
        threshold = 0.2 * time_diff_minutes
        
        if delays[i-1] > 10 and delays[i] == 0 and delays[i-1] - delays[i] > threshold:
            if i < len(delays) - 1:
                delays[i] = (delays[i-1] + delays[i+1]) // 2
            else:
                delays[i] = delays[i-1]
            change_count += 1
    if change_count > 0:
        #print(f"{old_delays} -> {delays}")
        pass
    return delays, change_count


def add_directions(train_data, is_incoming, debug=False):

    direction_list = [""] * len(train_data)
    directions = {'South': ['Weinheim(Bergstr)Hbf', 'Bruchsal', 'Karlsruhe-Durlach', 'Günzburg', 'Bensheim', 'Mannheim Hbf', 'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                        'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf', 'Offenburg', 'Freiburg(Breisgau) Hbf'],
              'West': ['Hamm(Westf)Hbf', 'Aachen Hbf', 'Mönchengladbach Hbf', 'Siegburg/Bonn', 'Hagen Hbf', 'Duisburg Hbf', 'Recklinghausen Hbf', 'Andernach', 'Köln/Bonn Flughafen', 'Solingen Hbf', 'Oberhausen Hbf', 'Montabaur', 'Münster(Westf)Hbf', 'Bochum Hbf', 'Wuppertal Hbf', 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                       'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf', 'Gelsenkirchen Hbf', 'Essen Hbf'],
              'North': ['Kassel-Wilhelmshöhe', 'Lüneburg', 'Göttingen', 'Hannover Messe/Laatzen', 'Uelzen', 'Hannover Hbf', 'Celle', 'Hamburg Dammtor', 'Neumünster', 'Treysa', 'Marburg(Lahn)', 'Gießen', 'Friedberg(Hess)', 'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kiel Hbf'],
              'North East': ['Weißenfels', 'Wittenberge', 'Naumburg(Saale)Hbf', 'Stendal Hbf', 'Halle(Saale)Hbf', 'Bitterfeld', 'Berlin Ostbahnhof','Berlin Südkreuz', 'Dresden-Neustadt', 'Wolfsburg Hbf', 'Eisenach', 'Dresden Hbf', 'Berlin-Spandau', 'Lutherstadt Wittenberg Hbf', 'Riesa', 'Hildesheim Hbf', 'Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf',
                             'Brandenburg Hbf', 'Magdeburg Hbf', 'Berlin Gesundbrunnen'],
              'East': ['München-Pasing', 'München Hbf', 'Augsburg Hbf', 'Plattling', 'Aschaffenburg Hbf', 'Passau Hbf', 'Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Ingolstadt Hbf']}
    
    direction_list = [""] * len(train_data)
    remove_indices = set()
    not_found = found = airport = count_impossible = 0

    for index, train_out in enumerate(train_data.itertuples()):
        direction_set = set()
        
        if is_incoming:
            stops = list(train_out.origin)
            stops.reverse()
        else:
            stops = list(train_out.destination)

        for dest in stops:
            for direction, stations in directions.items():
                if dest in stations:
                    direction_set.add(direction)
                    break

        if debug:
            if "South" in direction_set and ("West" in direction_set or "North" in direction_set or "North East" in direction_set):
                print(stops)
                count_impossible += 1
                print(index)
            if "East" in direction_set and ("West" in direction_set or "North" in direction_set):
                print(stops)
                print(train_out.departure)
                print(train_out.arrival)
                count_impossible += 1
                print(index)
                continue

        direction_list[index] = next(iter(direction_set), 'None')
        if direction_list[index] == 'None':
            not_found += 1
            if 'Frankfurt am Main Flughafen Fernbahnhof' in stops:
                airport += 1
        else:
            found += 1

    train_data['direction'] = direction_list

    
    # Remove indices found while debugging
    if is_incoming:
        remove_indices.update([35371, 35372, 88424])
    else:
        remove_indices.update([125187, 125225, 153510, 153822])
    train_data = train_data.drop(index=remove_indices)

    if debug:
        print(count_impossible)
    if is_incoming:
        print(f"Set directions of {found} incoming trains.")
        print(f"Did not find clear direction of {not_found} trains.")
        print(f"Out of those, {airport} trains end at Frankfurt airport without other stops.")
    else:
        print(f"Set directions of {found} outgoing trains.")
        print(f"Did not find clear direction of {not_found} trains.")
        print(f"Out of those, {airport} trains start at Frankfurt airport without other stops.")
    return train_data

data_in = pd.read_csv("data/scraped_incoming_Frankfurt_Hbf.csv",
                      names=['origin', 'destination', 'date', 'departure',
                             'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv("data/scraped_outgoing_Frankfurt_Hbf.csv",
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


# Set wrongly as 0 given delays to an interpolation
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

incoming.to_pickle("data/incoming.pkl")
outgoing.to_pickle("data/outgoing.pkl")
