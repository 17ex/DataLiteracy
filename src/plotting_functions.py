import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns




def read_data(main_folder_path, name_position=2, compare_gains=False):
    df_dict = {}

    if compare_gains:
        for foldername in os.listdir(main_folder_path):
            folder_path = os.path.join(main_folder_path, foldername)

            folder_dict = {}
            print(foldername)

            for filename in os.listdir(folder_path):
                if filename.endswith('.json'):
                    file_path = os.path.join(folder_path, filename)
                    
                    key_city = filename.split('_')[name_position].replace(".json", "")
                    key_city = key_city.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')

                    df = pd.read_json(file_path, orient='index')
                    df.index = df.index.str.replace('ä', 'ae').str.replace('ö', 'oe').str.replace('ü', 'ue')
                    folder_dict[key_city] = df
            
            df_dict[foldername] = folder_dict
    else:
        for filename in os.listdir(main_folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(main_folder_path, filename)
                

                key = filename.split('_')[name_position].replace(".json", "")

                key = key.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')

                df = pd.read_json(file_path, orient='index')
                df.index = df.index.str.replace('ä', 'ae').str.replace('ö', 'oe').str.replace('ü', 'ue')
                print(key)
                df_dict[key] = df
        
    return df_dict

def get_mean_delays(gain_dict, max_transfer_time=60, compare_gains=False):
    dict_mean_delays = {}
    if not compare_gains:
        gain_dict = {'avg_gain': gain_dict}

    for key_gain, df_dict in gain_dict.items():
        empty = pd.DataFrame(index=range(1, max_transfer_time + 1), columns=['mean_delay', 'reachable1', 'reachable2', 'reachable3'])
        empty.loc[:, ['reachable1', 'reachable2', 'reachable3']] = 0
        delay_matrix = [[] for _ in range(max_transfer_time)]

        for key, df in df_dict.items():

            for i in range(len(df)):
                row = df.iloc[i]
                valid_minutes = np.array(row['switch time']) <= max_transfer_time
                minutes = np.array(row['switch time'])[valid_minutes].astype(int)
                delays = np.array(row['delay'])[valid_minutes]

                for minute, delay in zip(minutes, delays):
                    delay_matrix[minute-1].append(delay)

                cases = np.array(row['reachable'])[valid_minutes]
                for case in [1, 2, 3]:
                    empty.loc[minutes[cases == case], f'reachable{case}'] += 1

        means = [np.mean(minute).round(2) if minute else np.nan for minute in delay_matrix]
        empty['mean_delay'] = means
        df_mean_delays = empty

        dict_mean_delays[key_gain] = df_mean_delays
    return dict_mean_delays