import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from pathlib import Path
import math


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
    # We define the date of a train as the date of departure.
    # If the train arrives a day later, it still has the date of departure
    # associated with it.
    df.loc[:, 'date'] = df.loc[:, 'departure'].apply(lambda d: d.date())


def all_equal(lst):
    return len(set(lst)) == 1


def remove_unequal_delays(df):
    """
    This should remove incoming trains from the dataset for which,
    after merging into one train with lists containing each station,
    the lists with the delay at Frankfurt Hbf contain different values,
    as this is impossible (it is the same train) and the data point
    must be wrong.
    """
    return df[df['delay'].apply(all_equal)]


def cancellation_to_int(s):
    """
    Returns 0 if input is na (no cancellation),
    1 if input specifies the train was cancelled at its origin station,
    2 if input specifies the train was cancelled at its destination.
    """
    if pd.isna(s):
        return 0
    if s == 'Ausfall (Startbahnhof)':
        return 1
    return 2


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
    arrivals = row['arrival']
    for i in range(1, len(delays)):
        # Calculate time difference in minutes
        time_diff_minutes = (arrivals[i] - arrivals[i-1]).total_seconds() / 60
        # TODO explain 0.27
        threshold = 0.27 * time_diff_minutes

        if delays[i-1] > 10 and delays[i] == 0 and delays[i-1] - delays[i] > threshold:
            if i < len(delays) - 1:
                delays[i] = (delays[i-1] + delays[i+1]) // 2
            else:
                delays[i] = delays[i-1]
            change_count += 1
    return delays, change_count


def add_directions(train_data, is_incoming, debug=False):
    """
    Determine the direction a train is taking.
    Directions are one of the five:
    South, West, North, North East, East.
    Returns a dataframe with an additional direction column
    containing the appropriate direction.
    """
    direction_list = [""] * len(train_data)
    # TODO
    # There is a function somewhere that contains this too.
    # Use that.
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
    # TODO
    # How were these indices found?
    # Replace with checks instead of magic numbers
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


def format_station_name_file(input_string):
    output_string = input_string.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
    output_string = output_string.replace('/', '-')
    return output_string


def unique_station_names(data_in, data_out):
    incoming_stations = set(data_in["origin"])
    outgoing_stations = set(data_out["destination"])
    stations = incoming_stations.union(outgoing_stations)
    # Rename a few stations as the 2 datasets use different names for these
    stations.add("Frankfurt(Main)Hbf")
    stations.add("Frankfurt(M) Flughafen Fernbf")
    stations.add("Stendal")
    stations.add("Hamm(Westf)")
    stations.remove('Hamm(Westf)Hbf')
    stations.remove('Frankfurt am Main Flughafen Fernbahnhof')
    stations.remove('Stendal Hbf')
    return stations


def haversine(coord1, coord2):
    # Radius of the Earth in km
    R = 6371.0
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    lat1 = float(lat1.replace(',', '.'))
    lon1 = float(lon1.replace(',', '.'))
    lat2 = float(lat2.replace(',', '.'))
    lon2 = float(lon2.replace(',', '.'))
    # Convert latitude and longitude from degrees to radians
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    # Haversine formula
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    # Distance in kilometers
    distance = R * c
    return distance


def pair_exclusion_criterion(origin_coords, destination_coords, frankfurt_coords):
    return (
        haversine(origin_coords, destination_coords) * 1.5
        < (haversine(frankfurt_coords, origin_coords)
           + haversine(frankfurt_coords, destination_coords))
    )


def write_excluded_station_pairs(station_coords, stations, filename):
    station_coords = station_coords[station_coords['NAME'].isin(stations)]
    frankfurt_coords = station_coords[
            station_coords["NAME"] == "Frankfurt(Main)Hbf"] \
                    .iloc[0][['Laenge', 'Breite']]
    station_pairs = station_coords.merge(station_coords, how='cross')

    def exclusion_fun(coord_pair):
        return pair_exclusion_criterion(
                (coord_pair['Laenge_x'], coord_pair['Breite_x']),
                (coord_pair['Laenge_y'], coord_pair['Breite_y']),
                frankfurt_coords)

    station_pairs = station_pairs[
            station_pairs.apply(exclusion_fun, axis=1)
            ]
    station_pairs[['NAME_x', 'NAME_y']] \
        .rename(columns={'NAME_x': 'origin', 'NAME_y': 'destination'}) \
        .to_csv(filename, index=False)


def load_excluded_pairs(data_dir):
    """
    Returns a set containing tuples (origin, destination)
    of station names that should be ignored in the analysis.
    """
    excluded_pairs = set()
    for _, origin, destination in pd.read_csv(data_dir + "excluded_pairs.csv").itertuples():
        excluded_pairs.add((origin, destination))
    return excluded_pairs
