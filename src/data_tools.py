"""
This file contains functions that work directly on data.
Almost all of it is used for data preprocessing in the
preprocessing.py script, but a few of these functions
are also used in the experiments.
"""
import os
import math
import numpy as np
import pandas as pd
from datetime import timedelta
from datetime import datetime
from pathlib import Path
import data_io


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


def determine_train_direction(train_data, is_incoming, debug=False):
    """
    Determine the direction a train is taking.
    Directions are one of the five:
    South, West, North, North East, East.
    Returns a dataframe with an additional direction column
    containing the appropriate direction.
    """
    directions = data_io.load_directions()
    direction_list = [""] * len(train_data)
    remove_indices = set()
    not_found = found = airport = count_unclear = 0

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
        if ("South" in direction_set
            and ("West" in direction_set
                 or "North" in direction_set
                 or "North East" in direction_set)
            or ("East" in direction_set
                and ("West" in direction_set
                     or "North" in direction_set))):
            if debug:
                    print("############")
                    print(index)
                    print(stops)
                    print(train_out.departure)
                    print(train_out.arrival)
                    count_unclear += 1
            direction_set = set()

        direction_list[index] = next(iter(direction_set), 'None')
        if direction_list[index] == 'None':
            not_found += 1
            if 'Frankfurt am Main Flughafen Fernbahnhof' in stops:
                airport += 1
        else:
            found += 1
    train_data['direction'] = direction_list
    if debug:
        print(count_unclear)
    if is_incoming:
        print(f"Set directions of {found} incoming trains.")
        print(f"Did not find clear direction of {not_found} trains.")
        print(f"Out of those, {airport} trains end at Frankfurt airport without other stops.")
    else:
        print(f"Set directions of {found} outgoing trains.")
        print(f"Did not find clear direction of {not_found} trains.")
        print(f"Out of those, {airport} trains start at Frankfurt airport without other stops.")
    return train_data


def fix_duplicate_frankfurt(data_in, data_out):
    data_in = data_in[data_in['origin'] != 'Frankfurt(Main)Hbf']
    data_out = data_out[data_out['destination'] != 'Frankfurt(Main)Hbf']
    return data_in, data_out


def remove_wrong_incoming_trains(incoming):
    """
    Remove trains from the dataset that contain wrong datapoints.
    """
    is_wrong = incoming['origin'].apply(
            lambda origins:
            len(origins) > 0
            and "Frankfurt" not in origins[-1]
            and any(map(lambda origin: "Frankfurt" in origin, origins)))
    print(f"Dropping {sum(is_wrong)} wrong incoming trains from the dataset.")
    return incoming[~is_wrong]


def remove_wrong_outgoing_trains(outgoing):
    """
    Remove trains from the dataset that contain wrong datapoints
    found by manual inspection.
    """
    out_ids_of_wrong_trains = \
        [
            321822,
            326938,
            193134,
            193205,
            265197,
            265739
        ]
    print(f"Dropping {len(out_ids_of_wrong_trains)} wrong outgoing \
            trains from the dataset.")
    return outgoing[outgoing['out_id'].apply(
        lambda out_id: out_id not in out_ids_of_wrong_trains)]


# TODO move to data_io
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


# TODO just return the pairs, move write to data_io
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


def load_excluded_pairs():
    """
    Returns a set containing tuples (origin, destination)
    of station names that should be ignored in the analysis.
    """
    filename = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                             os.pardir,
                                             "dat",
                                             "excluded_pairs.csv"))
    excluded_pairs = set()
    for _, origin, destination in pd.read_csv(filename).itertuples():
        excluded_pairs.add((origin, destination))
    return excluded_pairs
