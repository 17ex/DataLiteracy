"""
This file contains functions that write or load data.
"""
import os
import sys
import numpy as np
import pandas as pd
import json
import pickle
import requests
from pathlib import Path

REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                          os.pardir))
DATA_DIR = os.path.join(REPO_ROOT, 'dat')
TRAIN_DATA_DIR = os.path.join(DATA_DIR, 'train_data', 'frankfurt_hbf')
STATION_NAMES_BASENAME = 'station_names.json'
GAIN_VALS_BASENAME = 'gain_values.json'
EXCLUDED_PAIRS_BASENAME = 'excluded_pairs.csv'
EXCLUDED_PAIRS_FILE = os.path.join(DATA_DIR, EXCLUDED_PAIRS_BASENAME)
COORDINATES_BASENAME = 'coordinates.csv'
COORDINATES_FILE = os.path.join(DATA_DIR, COORDINATES_BASENAME)


def load_error_msg(filepath, descr, from_repo):
    print(f"Could not find the {descr} file.")
    print(f"It should be located at {filepath}.")
    if from_repo:
        print("It should be contained in the git repo.")
        print("If the file is not present, \
                ensure you didn't accidentally delete it.")
    else:
        print("Please make sure you ran the preprocessing script first.")


def filename_escape(input_string):
    """
    Replaces potentially problematic characters for filenames
    """
    output_string = input_string.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
    output_string = output_string.replace('/', '-')
    return output_string


def write_json(content, basename, *dirs):
    """
    Writes a dict to a JSON file.
    The files are stored in a subdirectory of
    the repo data directory (dat)

    Args:
    - content: dictionary that should be saved
    - basename: string that specifies the basename of the file
    - *dirs: directories in which to save the file

    """
    out_dirname = os.path.join(DATA_DIR, *dirs)
    Path(out_dirname).mkdir(parents=True, exist_ok=True)
    full_filename = os.path.join(out_dirname, basename)
    with open(full_filename, 'w', encoding='utf-8') as file:
        json.dump(content, file)


def write_unique_station_names(incoming, outgoing):
    """
    Writes a file in the data directory containing
    the names of the train stations we considered.
    Names are stored separately for stations from which
    a train connection exists to Frankfurt(Main)Hbf,
    from Frankfurt(Main)Hbf, and all stations.
    """
    unique_stations = set()
    unique_stations_in = set()
    unique_stations_out = set()
    incoming['origin'].apply(lambda sl: unique_stations_in.update(sl))
    outgoing['destination'].apply(lambda sl: unique_stations_out.update(sl))
    unique_stations = unique_stations_in.union(unique_stations_out)
    if 'Frankfurt(Main)Hbf' in unique_stations_in:
        unique_stations_in.remove('Frankfurt(Main)Hbf')
    if 'Frankfurt(Main)Hbf' in unique_stations_out:
        unique_stations_out.remove('Frankfurt(Main)Hbf')
    write_json(
            {
                "in": list(unique_stations_in),
                "out": list(unique_stations_out),
                "all": list(unique_stations)
            },
            STATION_NAMES_BASENAME)


def write_gain_vals(all_gains):
    """
    Saves the gain estimates in the data directory.
    """
    median_gain = {}
    average_gain = {}
    max_gain = {}
    pos_avg_gain = {}
    for key in all_gains.keys():
        median_gain[key] = np.median(all_gains[key]).item()
        average_gain[key] = np.mean(all_gains[key]).item()
        max_gain[key] = np.amax(all_gains[key]).item()
        positive_numbers = [num for num in all_gains[key] if num > 0]
        pos_avg_gain[key] = \
            np.mean(positive_numbers).item() if len(positive_numbers) > 0 else 0
    write_json(
            {
                "median": median_gain,
                "average": average_gain,
                "max": max_gain,
                "pos_avg": pos_avg_gain
            },
            GAIN_VALS_BASENAME)

def download_coordinates_file():
    """
    Downloads and stores a file containing information about
    German train stations. See below for the source URL.
    """
    if not Path(COORDINATES_FILE).is_file():
        coordinates_file_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
        print(f"Downloading station files from: {coordinates_file_url}")
        resp = requests.get(coordinates_file_url, timeout=10)
        if resp.ok:
            with open(COORDINATES_FILE, mode="wb") as file:
                file.write(resp.content)
        else:
            print("WARNING: Download of coordinates.csv failed!")
            print(f"Server replied status {resp.status_code}")
            sys.exit(1)
    else:
        print("Station coordinate file already exists, skipping.")


def write_excluded_station_pairs(excluded_pairs):
    excluded_pairs.to_csv(EXCLUDED_PAIRS_FILE, index=False)


def write_incoming_outgoing_conns(incoming, outgoing):
    Path(TRAIN_DATA_DIR).mkdir(parents=True, exist_ok=True)
    incoming.to_pickle(os.path.join(TRAIN_DATA_DIR, "incoming.pkl"))
    outgoing.to_pickle(os.path.join(TRAIN_DATA_DIR, "outgoing.pkl"))


def load_incoming_outgoing_conns():
    """
    Returns the incoming and outgoing train connections
    to Frankfurt Hbf.

    Returns:
    - incoming: Pandas dataframe containing trains
        that arrive at Frankfurt Hbf
    - outgoing: Pandas dataframe containing trains
        that depart from Frankfurt Hbf
    """
    filepath_inc = os.path.join(TRAIN_DATA_DIR, 'incoming.pkl')
    filepath_out = os.path.join(TRAIN_DATA_DIR, 'outgoing.pkl')
    try:
        with open(filepath_inc, 'rb') as file:
            incoming = pickle.load(file)
        with open(filepath_out, 'rb') as file:
            outgoing = pickle.load(file)
        return incoming, outgoing
    except FileNotFoundError:
        load_error_msg(filepath_inc, "train database", False)
        raise


def load_station_subset():
    """
    Returns a python set containing a hand-selected list of stations,
    for which some kind of analysis should be performed exclusively.

    Returns:
    - station_subset: Set containing the station names as strings
    """
    filepath = os.path.join(DATA_DIR, 'station_subset.json')
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return set(json.loads(file.read()))
    except FileNotFoundError:
        load_error_msg(filepath, "station subset json", True)
        raise


def load_directions():
    """
    Returns pre-defined 5 directions
    (South, West, North, North East and East)
    in which train stations can lie relative to Frankfurt(Main)Hbf.

    Returns:
    - directions: Dict containing as keys the directions, and as
        values lists containing the corresponding train station names
    """
    filepath = os.path.join(DATA_DIR, 'directions.json')
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.loads(file.read())
    except FileNotFoundError:
        load_error_msg(filepath, "directions json", True)
        raise


def load_unique_station_names():
    """
    Returns the train station names for all stations that
    have a train coming to, going to, and both, Frankfurt Hbf

    Returns:
    - incoming_names: Dict containing as keys the directions, and as
        values lists containing the corresponding train station names
    """
    filepath = os.path.join(DATA_DIR, STATION_NAMES_BASENAME)
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            sn_dict = json.loads(file.read())
            return set(sn_dict['in']), set(sn_dict['out']), set(sn_dict['all'])
    except FileNotFoundError:
        load_error_msg(filepath, "station names json", False)
        raise


def load_gain_values(gain_key, return_all=False):
    """
    Returns a dict containing gain estimates.
    """
    filepath = os.path.join(DATA_DIR, GAIN_VALS_BASENAME)
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            gain_dict = json.loads(file.read())
            if return_all:
                return gain_dict
            try:
                return gain_dict[gain_key]
            except KeyError as exc:
                print("Invalid argument")
                print("Argument gain_key has to be one of the following:")
                print(gain_dict.keys())
                raise RuntimeError from exc
    except FileNotFoundError:
        load_error_msg(filepath, "directions json", True)
        raise


def load_station_coordinates():
    """
    Returns a dataframe containing train stations
    and their geographical coordinates.
    """
    return pd.read_csv(COORDINATES_FILE, sep=';',
                       usecols=['NAME', 'Laenge', 'Breite'])


def load_excluded_pairs():
    """
    Returns a set containing tuples (origin, destination)
    of station names that should be ignored in the analysis.
    """
    excluded_pairs = set()
    for _, origin, destination in pd.read_csv(EXCLUDED_PAIRS_FILE).itertuples():
        excluded_pairs.add((origin, destination))
    return excluded_pairs
