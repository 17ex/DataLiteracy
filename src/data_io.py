"""
This file contains functions that write or load data.
"""
import os
from pathlib import Path
import json
import pickle

REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                          os.pardir))
DATA_DIR = os.path.join(REPO_ROOT, 'dat')
TRAIN_DATA_DIR = os.path.join(DATA_DIR, 'train_data', 'frankfurt_hbf')
STATION_NAMES_BASENAME = 'station_names.json'


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
    unique_stations = set()
    unique_stations_in = set()
    unique_stations_out = set()
    incoming['origin'].apply(lambda sl: unique_stations_in.update(sl))
    outgoing['destination'].apply(lambda sl: unique_stations_out.update(sl))
    unique_stations = unique_stations_in.union(unique_stations_out)
    unique_stations_in.remove('Frankfurt(Main)Hbf')
    unique_stations_out.remove('Frankfurt(Main)Hbf')
    write_json(
            {
                "in": list(unique_stations_in),
                "out": list(unique_stations_out),
                "all": list(unique_stations)
            },
            STATION_NAMES_BASENAME)


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
    try:
        with open(os.path.join(TRAIN_DATA_DIR, 'incoming.pkl'), 'rb') as file:
            incoming = pickle.load(file)
        with open(os.path.join(TRAIN_DATA_DIR, 'outgoing.pkl'), 'rb') as file:
            outgoing = pickle.load(file)
        return incoming, outgoing
    except FileNotFoundError:
        print("Could not find the train database files.")
        print("Please make sure you ran the preprocessing script first.")
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
        print("Could not find the station subset json file.")
        print(f"It should be located at {filepath}.")
        print("It should be contained in the git repo.")
        print("If the file is not present, \
                ensure you didn't accidentally delete it.")
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
        print("Could not find the directions json file.")
        print(f"It should be located at {filepath}.")
        print("It should be contained in the git repo.")
        print("If the file is not present, \
                ensure you didn't accidentally delete it.")
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
        print("Could not find the station names json file.")
        print(f"It should be located at {filepath}.")
        print("Please make sure you ran the preprocessing script first.")
        raise
