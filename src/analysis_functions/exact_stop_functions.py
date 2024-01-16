from src.analysis_functions.general_functions import reachable_train


def find_next_train(train, next_trains, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Finds the next train based on certain criteria.

    Args:
    - train (DataFrame): DataFrame containing information about the current train.
    - next_trains (DataFrame): DataFrame containing information about the trains that could be taken
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - next_train (Next Train in the Dataframe or None): The next train information, if found; otherwise, None.
    - time_difference (float): Time difference in minutes between the next train's departure and the current train's plan_departure.
    """
    dest_idx = train.destination_idx
    plan_arrival = train.arrival_y[dest_idx]
    # only look at trains that arrive later at the destination
    # TODO sort first somewhere (maybe outside this function).
    # Then the next operations can maybe be replaced if iterating in order, which would be way faster.
    next_trains = next_trains[
        # next_trains.apply(lambda row: row['arrival_y'][row['destination_idx']] > plan_arrival, axis=1)]
        next_trains.apply(lambda row: row['arrival_y'][row['destination_idx']] > plan_arrival, axis=1)]
    while not next_trains.empty:
        next_train_idx = next_trains.apply(lambda row: row['arrival_y'][row['destination_idx']], axis=1).idxmin()
        next_train = next_trains.loc[next_train_idx]
        dest_idx = next_train.destination_idx
        origin_idx = int(next_train.origin_idx)
        cancellation_out = next_train.cancellation_y
        cancellation_in = next_train.cancellation_x
        plan_difference, delay_difference = reachable_train(next_train, gains, estimated_gain, worst_case)
        if (
                cancellation_in[origin_idx] != 0 or
                cancellation_in[-1] != 0 or
                plan_difference <= delay_difference or
                cancellation_out[dest_idx] != 0
           ):
            # If it is impossible to take this train (eg. it was canceled)
            next_trains = next_trains.drop(next_train_idx)
        else:
            return next_train, (next_train.arrival_y[dest_idx] - plan_arrival).total_seconds() / 60, dest_idx
    return None, 0, 0


def reachable_transfers(incoming_from_origin, outgoing, origin, destination, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Identifies reachable transfers between incoming and outgoing trains.

    Args:
    - incoming_from_origin (DataFrame): DataFrame containing trains that go from origin to Frankfurt
    - outgoing (DataFrame): DataFrame containing outgoing train information.
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - reachable_count (dict): Count of reachable and unreachable transfers based on plan and delay differences.
    - delay (dict): Average delay information for each plan difference.
    """
    max_hours = 3
    max_delay_minutes = max_hours * 60
    outgoing_to_dest = outgoing[outgoing['destination'] \
            .apply(lambda destinations: destination in destinations)]
    candidate_transfers = incoming_from_origin.merge(outgoing_to_dest, how='outer', on='date')
    # Filter out trains that go from origin to destination directly,
    # as there is no train transfer in that case.
    candidate_transfers = candidate_transfers[candidate_transfers['in_id_x'] != candidate_transfers['in_id_y']]
    candidate_transfers['transfer_time'] = \
        (candidate_transfers['departure_y'] - candidate_transfers['arrival_x']) \
            .dt.total_seconds() / 60
    # Filter out trains that one can not transfer to, because they departed
    # before the incoming train arrived, or because it would take too long
    # (Restriction: may not take longer than max_hours)
    candidate_transfers = candidate_transfers[
            (candidate_transfers['transfer_time'] > 0) &
            (candidate_transfers['transfer_time'] <= max_delay_minutes)]
    candidate_transfers.loc[:, 'destination_idx'] = \
            candidate_transfers['destination_y'].apply(lambda destinations: destinations.index(destination))
    # TODO
    # With  origin Karlsruhe Hbf, Destination Berlin Hbf, I get weird results
    # (or I'm misunderstanding this). investigate this
    # TODO above, figure out which combinations are gonna be a problem when looking for transfers over 12pm.
    # probably only trains that started after 12pm
    delay = {'switch time': [], 'date': [], 'delay': [], 'reachable': []}
    discarded = 0
    unique_ids = candidate_transfers['in_id_x'].unique()
    # Counters for a quick sanity check. Not used further in the analysis.
    found_train_x = 0
    not_found_x = 0
    found_train_y = 0
    not_found_y = 0
    for incoming_train_id in set(unique_ids):
        group_id = candidate_transfers[candidate_transfers['in_id_x'] == incoming_train_id]
        example_train = group_id.iloc[0]
        group_date = candidate_transfers[candidate_transfers['date'] == example_train['date']]
        for train in group_id.itertuples():
            if train.transfer_time > 60:
                discarded += 1
                continue
            origin_idx = int(train.origin_idx)
            dest_idx = train.destination_idx
            plan_arrival = train.arrival_y[dest_idx]
            arrival_FRA = train.arrival_fra
            plan_departure_origin = train.departure_origin
            plan_difference, delay_difference = reachable_train(train, gains, estimated_gain, worst_case)
            delay['switch time'].append(plan_difference)
            delay['date'].append(plan_arrival.strftime('%Y-%m-%d %H:%M:%S'))

            if train.cancellation_x[origin_idx] != 0 or train.cancellation_x[-1] != 0:
                # If the train that should arrive in Frankfurt was cancelled
                # Find the next train going from origin to Frankfurt as alternative
                delay['reachable'].append(1)
                # filtering so these trains have a planned departure at the origin after the original train
                candidate_connections_to_framkfurt = \
                    group_date[group_date['departure_origin'] > plan_departure_origin]
                next_train, extra_delay, dest_idx = \
                    find_next_train(train, candidate_connections_to_framkfurt, gains, estimated_gain, worst_case)
                if next_train:
                    found_train_x += 1
                    delay['delay'].append(next_train.delay_y[dest_idx] + extra_delay)
                else:
                    not_found_x += 1
                    delay['delay'].append(max_delay_minutes)
            elif train.cancellation_y[dest_idx] != 0 or plan_difference <= delay_difference:
                # If the departing train was cancelled or transfer to it is impossible
                delay['reachable'].append(2)
                # only look at trains that leave later in Frankfurt
                candidate_departing_trains = group_id[group_id['departure_y'] > arrival_FRA]
                next_train, extra_delay, dest_idx = \
                    find_next_train(train, candidate_departing_trains, gains, estimated_gain, worst_case)
                if next_train:
                    found_train_y += 1
                    delay['delay'].append(next_train.delay_y[dest_idx] + extra_delay)
                else:
                    not_found_y += 1
                    delay['delay'].append(max_delay_minutes)
            else:
                # If it was possible to take the connecting train as planned
                delay['reachable'].append(3)  # TODO why not 0?
                delay['delay'].append(train.delay_y[dest_idx])
    if len(candidate_transfers) > 0:
        print(discarded / len(candidate_transfers))
    if found_train_x > 0 and found_train_y > 0:
        print(found_train_x, not_found_x, found_train_y, not_found_y)
    return delay
