from .general_functions import reachable_train


def find_next_train(train, filtered_next, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Finds the next train based on certain criteria.

    Args:
    - train (DataFrame): DataFrame containing information about the current train.
    - all_trains (DataFrame): DataFrame containing information about all trains.
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - next_train (Next Train in the Dataframe or None): The next train information, if found; otherwise, None.
    - time_difference (float): Time difference in minutes between the next train's departure and the current train's plan_departure.
    """
    dest_idx = train.destination_idx
    plan_arrival = train.arrival_y[dest_idx]
    # only look at trains that arrive later in the destination
    filtered_next = filtered_next[
        filtered_next.apply(lambda row: row['arrival_y'][row['destination_idx']] > plan_arrival, axis=1)]
    while not filtered_next.empty:
        #TODO: are all cancellations considered?
        next_train_idx = filtered_next.apply(lambda row: row['arrival_y'][row['destination_idx']], axis=1).idxmin()
        next_train = filtered_next.loc[next_train_idx]
        dest_idx = next_train.destination_idx
        cancellation_out = next_train.cancellation_y
        plan_difference, delay_difference = reachable_train(next_train, gains, estimated_gain, worst_case)
        if plan_difference <= delay_difference or cancellation_out[dest_idx] != 0:
            filtered_next = filtered_next.drop(next_train_idx)
        else:
            return next_train, (next_train.arrival_y[dest_idx] - plan_arrival).total_seconds() / 60, dest_idx
    del filtered_next
    return None, 0, 0


def reachable_transfers(incoming_origin, outgoing, origin, destination, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Identifies reachable transfers between incoming and outgoing trains.

    Args:
    - incoming (DataFrame): DataFrame containing incoming train information.
    - outgoing (DataFrame): DataFrame containing outgoing train information.
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - reachable_count (dict): Count of reachable and unreachable transfers based on plan and delay differences.
    - delay (dict): Average delay information for each plan difference.
    """
    max_hours = 3
    outgoing_dest = outgoing[outgoing['destination'].apply(lambda x: any(destination == value for value in x))]
    filtered = incoming_origin.merge(outgoing_dest, how='outer', on='date')
    filtered['time_difference'] = (filtered['departure_y'] - filtered['arrival_x']).dt.total_seconds() / 60
    filtered = filtered[(filtered['arrival_x'] < filtered['departure_y']) & (filtered['time_difference'] <= max_hours * 60)
                        & (filtered['in_id_x'] != filtered['in_id_y'])]
    filtered.loc[:, 'destination_idx'] = filtered['destination_y'].apply(lambda x: x.index(destination))
    reachable_count = [[], [], []]
    delay = [[], [], []]
    discarded = 0
    unique_ids = filtered['in_id_x'].unique()
    found_train_x = 0
    not_found_x = 0
    found_train_y = 0
    not_found_y = 0
    for id in set(unique_ids):
        group_id = filtered[filtered['in_id_x'] == id]
        example_train = group_id.iloc[0]
        group_date = filtered[filtered['date'] == example_train['date']]
        for train in group_id.itertuples():
            if train.time_difference > 60:
                discarded += 1
                continue
            origin_idx = int(train.origin_idx)
            dest_idx = train.destination_idx
            plan_arrival = train.arrival_y[dest_idx]
            plan_departure_FRA = train.departure_y
            plan_departure_origin = train.departure_origin
            plan_difference, delay_difference = reachable_train(train, gains, estimated_gain, worst_case)
            delay[0].append(plan_difference)
            delay[1].append(plan_arrival)
            reachable_count[0].append(plan_difference)
            reachable_count[1].append(plan_arrival)

            # case if the stops of the arriving train were cancelled
            if train.cancellation_x[origin_idx] != 0 or train.cancellation_x[-1] != 0:
                reachable_count[2].append(1)
                # filtering so these trains have a planned departure at the origin after the original train
                filtered_next = group_date[group_date['departure_origin'] > plan_departure_origin]
                next_train, extra_delay, dest_idx = find_next_train(train, filtered_next, gains, estimated_gain, worst_case)
                if next_train is not None:
                    found_train_x += 1
                    delay[2].append(next_train.delay_y[dest_idx] + extra_delay)
                else:
                    not_found_x += 1
                    delay[2].append(max_hours * 60)
            # case if the stops of the leaving train were cancelled or transfer not possible
            #TODO: cancellation frankfurt
            elif train.cancellation_y[dest_idx] != 0 or plan_difference <= delay_difference:
                reachable_count[2].append(2)
                # only looking at trains that leave later in Frankfurt
                #TODO: make this the actual arrival time in Frankfurt
                filtered_next = group_id[group_id['departure_y'] > plan_departure_FRA]
                next_train, extra_delay, dest_idx = find_next_train(train, filtered_next, gains, estimated_gain, worst_case)
                if next_train is not None:
                    found_train_y += 1
                    delay[2].append(next_train.delay_y[dest_idx] + extra_delay)
                else:
                    not_found_y += 1
                    delay[2].append(max_hours * 60)
            # case if train was reachable
            else:
                reachable_count[2].append(3)
                delay[2].append(train.delay_y[dest_idx])
    if len(filtered) > 0:
        print(discarded / len(filtered))
    if found_train_x > 0 and found_train_y > 0:
        print(found_train_x, not_found_x, found_train_y, not_found_y)
    return reachable_count, delay
