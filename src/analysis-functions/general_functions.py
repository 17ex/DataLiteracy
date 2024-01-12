import pandas as pd


def find_biggest_gain_per_next_stop(incoming, outgoing):
    """
    Finds the biggest gain per next stop based on incoming and outgoing train data.
    A gain is positive if the train was faster than it was planned and negative if it was slower.

    Args:
    - incoming (DataFrame): DataFrame containing incoming train information.
    - outgoing (DataFrame): DataFrame containing outgoing train information.

    Returns:
    - gains (dict): Dictionary containing the biggest gain per next stop.
    - average_gain (dict): Dictionary containing average gains per destination.
    """

    gains = {}  # Dictionary to store the biggest gain per next stop
    average_gain = {}  # Dictionary to store average gains per next stop
    merged = pd.merge(incoming, outgoing, on='in_id', how='inner')
    acc_too_large = 0
    acc_normal = 0
    acc_cancelled = 0

    for row in merged.itertuples():
        # Extracting relevant information from the merged DataFrame
        departure = row.departure_y
        arrival = row.arrival_x
        delay_in = row.delay_x
        delay_out = row.delay_y[0]
        destination = row.destination_y[0]

        # Skipping rows with cancellations, as they don't contain useful gain information
        if 1 in row.cancellation_x or 2 in row.cancellation_x or 1 in row.cancellation_y or 2 in row.cancellation_y:
            acc_cancelled += 1
            continue

        driving_time = (row.arrival_y[0] - departure).total_seconds() / 60
        wait_time = (departure - arrival).total_seconds() / 60
        departure_delay = max(0, delay_in - wait_time)
        gain = departure_delay - delay_out

        # Handling cases where gain exceeds a threshold of 10% of the time the train takes
        if gain > 0.1 * driving_time:
            delays = row.delay_y
            delay_out = -1
            # adjust for the errors in the data where large delays go to 0 and then back up to the actual delay
            for j in range(len(delays)):
                if delays[j] != 0:
                    delay_out = delays[j]
                    destination = row.destination_y[j]
                    break
            if delay_out != -1:
                gain = departure_delay - delay_out
            else:
                gain = 0
            acc_too_large += 1
            continue
        else:
            acc_normal += 1

        if destination not in gains.keys():
            gains[destination] = max(0, gain)
            average_gain[destination] = (1, gain)
        else:
            gains[destination] = max(gains[destination], gain)
            t = average_gain[destination][0]
            v = average_gain[destination][1]
            average_gain[destination] = (t + 1, (t * v + gain) / (t + 1))

    return gains, average_gain


def reachable_train(train, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Calculates the plan difference and delay difference for a given train.

    Args:
    - train (row of a DataFrame): DataFrame containing information about the train.
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - plan_difference (float): Time difference between departure and arrival at a specific station.
    - delay_difference (float): Difference between the planned delay and the actual delay.
    """
    arrival_FRA = train.arrival_x
    departure_FRA = train.departure_y
    in_delay = train.delay_x
    dest_delay = train.delay_y
    dest_arrival = train.arrival_y[0]
    destination = train.destination_y[0]
    plan_difference = (departure_FRA - arrival_FRA).total_seconds() / 60

    if worst_case:
        delay_difference = in_delay
    elif gains:
        gain = gains.get(destination, 0)
        out_delay = max(0, dest_delay[0] + gain)
        delay_difference = max(0, in_delay - out_delay)
    else:
        estimated_gain * (dest_arrival - departure_FRA).total_seconds() / 60
        out_delay = max(0, dest_delay[0] + estimated_gain)
        delay_difference = max(0, in_delay - out_delay)
    return plan_difference, delay_difference
