import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from pathlib import Path

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
        threshold = 0.27 * time_diff_minutes
        
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