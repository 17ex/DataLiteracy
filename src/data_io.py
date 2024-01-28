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
    out_dirname = DATA_DIR
    for subdir in dirs:
        out_dirname = os.path.join(out_dirname, subdir)
    full_filename = os.path.join(out_dirname, basename)
    Path(out_dirname).mkdir(parents=True, exist_ok=True)
    # TODO add file encoding
    with open(full_filename, 'w') as file:
        json.dump(content, file)


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
