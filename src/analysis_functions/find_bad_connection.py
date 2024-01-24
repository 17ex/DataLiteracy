import pandas as pd
import numpy as np
import json
import pickle
from pathlib import Path
import sys
from datetime import timedelta, datetime
from general_functions import get_directions
import math


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

INPUT_DIR = "../../dat/scraped/"
OUTPUT_DIR = "../../dat/train_data/frankfurt_hbf/"
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


data_in = pd.read_csv(INPUT_DIR + "scraped_incoming_Frankfurt_Hbf.csv",
                      names=['origin', 'destination', 'date', 'departure',
                             'arrival', 'train', 'delay', 'cancellation'])
data_out = pd.read_csv(INPUT_DIR + "scraped_outgoing_Frankfurt_Hbf.csv",
                       names=['origin', 'destination', 'date', 'departure',
                              'arrival', 'train', 'delay', 'cancellation'])

incoming_stations = set(data_in["origin"])
outgoing_stations = set(data_out["destination"])
stations = incoming_stations.union(outgoing_stations)
stations.add("Frankfurt(Main)Hbf")
stations.add("Frankfurt(M) Flughafen Fernbf")
stations.add("Stendal")
stations.add("Hamm(Westf)")
stations.remove('Hamm(Westf)Hbf')
stations.remove('Frankfurt am Main Flughafen Fernbahnhof')
stations.remove('Stendal Hbf')

columns_to_use = ['NAME', 'Laenge', 'Breite']
df = pd.read_csv("../../dat/coordinates.csv", sep=';', usecols=columns_to_use)
filtered_df = df[df['NAME'].isin(stations)]

frankfurt_coords = filtered_df[filtered_df["NAME"] == "Frankfurt(Main)Hbf"].iloc[0][['Laenge', 'Breite']]
ignore_pairs = []
for station1 in filtered_df["NAME"]:
    for station2 in filtered_df["NAME"]: 
        if station1 != station2:
            station1_row = filtered_df[filtered_df["NAME"] == station1]
            station2_row = filtered_df[filtered_df["NAME"] == station2]

            if not station1_row.empty and not station2_row.empty:
                station1_coords = station1_row.iloc[0][['Laenge', 'Breite']]
                station2_coords = station2_row.iloc[0][['Laenge', 'Breite']]

            if haversine(station1_coords, station2_coords) * 1.5 < \
                haversine(frankfurt_coords, station1_coords) + haversine(frankfurt_coords, station2_coords):
                if (station1, station2) not in ignore_pairs:
                    ignore_pairs.append((station1, station2))
    
print(ignore_pairs)
print(len(ignore_pairs) / 2)

