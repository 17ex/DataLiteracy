"""
This file contains functions that write or load data.
"""
import os
from pathlib import Path
import json
REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                          os.pardir))
DATA_DIR = os.path.join(REPO_ROOT, 'dat')

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



