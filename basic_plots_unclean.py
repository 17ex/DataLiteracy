import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt
from datetime import date
from tueplots.constants.color import rgb

# Load data
with open('data/incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open('data/outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

pd.set_option('display.max_columns', None)

# TODO Ignore cancelled trains' delays.
# Some very basic plots (daily/monthly delay percentages, delays after number of stops)

def get_daily_delay_percentage(replace_missing_data=np.nan, use_outgoing=True):
    percentages = []
    dates = []
    no_data_dates = []
    for year in [2021, 2022, 2023]:
        for month in range(1, 13):
            if month == 2:
                days = 28
            elif month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12:
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
                if use_outgoing:
                    data = outgoing[(outgoing["date"] >= date(year, month, 1)) & (outgoing["date"] < next_date)]
                else:
                    data = incoming[(incoming["date"] >= date(year, month, 1)) & (incoming["date"] < next_date)]
                late = 0
                punctual = 0
                for delays in data["delay"]:
                    if use_outgoing:
                        for entry in delays:
                            if entry >= 6:
                                late += 1
                            else:
                                punctual += 1
                    else:
                        if delays >= 6:
                            late += 1
                        else:
                            punctual += 1
                dates.append(date(year, month, day))
                if late + punctual == 0:
                    no_data_dates.append(date(year, month, day))
                    percentages.append(replace_missing_data)
                else:  
                    percentages.append(punctual / (late + punctual))
    if len(no_data_dates) > 0:
        print("No data on these days:")
        print(no_data_dates)
    return percentages, dates, no_data_dates


def get_monthly_delay_percentage(replace_missing_data=np.nan, use_outgoing=True):
    percentages = []
    dates = []
    no_data_dates = []
    for year in [2021, 2022, 2023]:
        for month in range(1, 13):
            if month < 12:
                next_date = date(year, month+1, 1)
            else:
                next_date = date(year+1, 1, 1)
            if use_outgoing:
                data = outgoing[(outgoing["date"] >= date(year, month, 1)) & (outgoing["date"] < next_date)]
            else:
                data = incoming[(incoming["date"] >= date(year, month, 1)) & (incoming["date"] < next_date)]
            late = 0
            punctual = 0
            for delays in data["delay"]:
                if use_outgoing:
                    for entry in delays:
                        if entry >= 6:
                            late += 1
                        else:
                            punctual += 1
                else:
                    if delays >= 6:
                        late += 1
                    else:
                        punctual += 1
            dates.append(date(year, month, 1))
            if late + punctual == 0:
                no_data_dates.append(date(year, month, 1))
                percentages.append(replace_missing_data)
            else:  
                percentages.append(punctual / (late + punctual))
    if len(no_data_dates) > 0:
        print("No data on these months:")
        print(no_data_dates)
    return percentages, dates


def plot(dates, percentages):
    plt.plot(dates, percentages, color='blue')
    plt.xlabel('Date')
    plt.ylabel('Percentage of punctual trains')
    if len(percentages) > 50:
        plt.title('Daily Percentage of Punctual Trains (delay <= 5)')
    else:
        plt.title('Monthly Percentage of Punctual Trains (delay <= 5)')
    plt.tight_layout()
    plt.show()

#percentages1, dates, no_data_dates = get_daily_delay_percentage(replace_missing_data=np.nan, use_outgoing=False)
#percentages2, dates, no_data_dates = get_daily_delay_percentage(replace_missing_data=np.nan, use_outgoing=True)
#percentages = np.array(percentages1) - np.array(percentages2)
#plot(dates, percentages)


def get_delay_array():

    delay_array = np.empty((len(outgoing["delay"]), 23))
    delay_array[:] = np.nan

    d = 0
    for delays in outgoing["delay"]:
        for i in range(len(delays)):
            if i < 23:
                delay_array[d, i] = delays[i]
        d += 1
    return delay_array    

def plot_stop_statistics(delay_array, lower_percentile=25, upper_percentile=75):
    labels = np.arange(24)
    labels = ["Fr", "+1", "+2", "+3", "+4", "+5", "+6", "+7", "+8", "+9", "+10", "+11", "+12", "+13", "+14", "+15", "+16", "+17", "+18", "+19", "+20", "+21", "+22", "+23"]
    means = np.zeros(24)
    stds = np.zeros(24)
    medians = np.zeros(24)
    percentile_lower = np.zeros(24)
    percentile_upper = np.zeros(24)

    means[0] = np.mean(incoming["delay"])
    means[1:] = np.nanmean(delay_array, axis=0)

    stds[0] = 0.0 * np.std(incoming["delay"])
    stds[1:] = 0.0 * np.nanstd(delay_array, axis=0)

    medians[0] = np.median(incoming["delay"])
    medians[1:] = np.nanmedian(delay_array, axis=0)

    percentile_lower[0] = np.percentile(incoming["delay"], lower_percentile)
    percentile_lower[1:] = np.nanpercentile(delay_array, lower_percentile, axis=0)

    percentile_upper[0] = np.percentile(incoming["delay"], upper_percentile)
    percentile_upper[1:] = np.nanpercentile(delay_array, upper_percentile, axis=0)


    plt.figure()
    plt.errorbar(labels, medians, yerr=[medians-percentile_lower, percentile_upper-medians], fmt='o', label=f'Median and {lower_percentile}-{upper_percentile}% quantiles', color=rgb.tue_red)
    plt.errorbar(labels, means, yerr=stds, fmt='o', label='Mean', color=rgb.tue_blue)
    plt.legend()
    plt.xlabel('Stops')
    plt.ylabel('Mean Delay in min')
    plt.title('Mean Delay at Stops starting at Frankfurt')
    plt.tight_layout()
    plt.show()

plot_stop_statistics(get_delay_array())




