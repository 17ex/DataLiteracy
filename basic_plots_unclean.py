import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt
from datetime import date

with open('data/incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open('data/outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

pd.set_option('display.max_columns', None)

# Some very basic plots (daily/monthly delay percentages, delays after number of stops)
# TODO clean this

def daily_monthly():
    late = 0
    punctual = 0
    percentages = []
    dates = []
    no_data = []
    #outgoing_nov23 = outgoing[(outgoing["date"] >= date(2023, 11, 1)) & (outgoing["date"] < date(2023, 12, 1))]
    for year in [2021, 2022, 2023]:
        for month in range(1, 13):
            if month == 2:
                days = 28
            elif month == 1 | month == 3 | month == 5 | month == 7 | month == 8 | month == 10 | month == 12:
                days = 31
            else:
                days = 30
            for day in range(1, days+1):
                if day < days:
                    next_date = date(year, month, day+1)
                elif month < 12:
                    next_date = date(year, month+1, 1)
                else:
                    next_date = date(year+1, 1, 1)
                next_date = date(year, month, day+1) if day < days else date(year+1, 1, 1)
                data = outgoing[(outgoing["date"] >= date(year, month, 1)) & (outgoing["date"] < next_date)]
                if year == 2023 and month == 7:
                    print("here")
                    print(data)
                    print("here")
                late = 0
                punctual = 0
                for delays in data["delay"]:
                    """if delays >= 6:
                        late += 1
                    else:
                        punctual += 1"""
                    for entry in delays:
                        if entry >= 6:
                            late += 1
                        else:
                            punctual += 1
                dates.append(date(year, month, day))
                if late + punctual == 0:
                    no_data.append(date(year, month, day))
                    percentages.append(0)
                else:
                    
                    percentages.append(punctual / (late + punctual))
    print(no_data)
    plt.plot(dates, percentages, color='blue')
    plt.xlabel('Date')
    plt.ylabel('Percentage of punctual trains')
    plt.title('Monthly Percentage of Punctual Trains (delay <= 5)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

#daily_monthly()

fra_delay = np.mean(incoming["delay"])

delay_arr = np.empty((len(outgoing["delay"]), 23))
delay_arr[:] = np.nan
d = 0

for delays in outgoing["delay"]:
    for i in range(len(delays)):
        if i < 23:
            delay_arr[d, i] = delays[i]
    d += 1

print(delay_arr)

labels = ["Frankfurt", "+1 stop", "+2 stops", "+3 stops", "+4 stops", "+5 stops", "+6 stops"]
labels = np.arange(23)
means = np.nanmean(delay_arr, axis=0)
stds = 0.0*np.nanstd(delay_arr, axis=0)

"""plt.bar(labels, means, yerr=stds)
plt.ylabel('Mean Delay in min')
plt.title('Mean Delay at Stops starting at Frankfurt')
plt.tight_layout()
plt.show()"""

medians = np.nanmedian(delay_arr, axis=0)
percentile_25 = np.nanpercentile(delay_arr, 25, axis=0)
percentile_75 = np.nanpercentile(delay_arr, 75, axis=0)

plt.figure()
plt.errorbar(labels, medians, yerr=[medians-percentile_25, percentile_75-medians], fmt='o', label='Median and 25-75% quantiles')
plt.errorbar(labels, means, yerr=stds, fmt='o', label='Mean')
plt.legend()
plt.ylabel('Mean Delay in min')
plt.title('Mean Delay at Stops starting at Frankfurt')
plt.tight_layout()

plt.show()
