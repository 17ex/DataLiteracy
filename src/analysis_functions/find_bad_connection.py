import pandas as pd
from pathlib import Path
import math

def load_excluded_pairs(filename):
    """
    Returns a set containing tuples (origin, destination)
    of station names that should be ignored in the analysis.
    """
    excluded_pairs = set()
    for _, origin, destination in pd.read_csv(filename).itertuples():
        excluded_pairs.add((origin, destination))
    return excluded_pairs
