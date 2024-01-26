from src.analysis_functions.general_functions import can_take_connecting_train, get_plan_and_delay_difference
import pandas as pd
from datetime import datetime, time, timedelta


def find_next_train(train, next_train_candidates, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Finds the next train based on certain criteria.

    Args:
    - train (Pandas): Pandas object containing information about the current train.
    - next_trains (DataFrame): DataFrame containing information about the trains that could be taken
    - gains (dict, optional): Dictionary containing gains. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - next_train (Next Train in the Dataframe or None): The next train information, if found; otherwise, None.
    - time_difference (float): Time difference in minutes between the next train's departure and the current train's plan_departure.
    """
    dest_idx = train.destination_idx
    plan_arrival = train.arrival_destination
    next_train_candidates = next_train_candidates[
            ~next_train_candidates['cancellation_inbound']
            & ~next_train_candidates['cancellation_outbound']
            ].sort_values(by=['arrival_destination'])
    while not next_train_candidates.empty:
        next_train = next_train_candidates.iloc[0]
        dest_idx = next_train.destination_idx
        if not can_take_connecting_train(
                    next_train, gains, estimated_gain, worst_case):
            next_train_candidates.drop(next_train_candidates.index[0], inplace=True)
        else:
            return next_train, (next_train.arrival_destination - plan_arrival).total_seconds() / 60, dest_idx
    return None, 0, 0


def add_columns(candidate_transfers, destination):
    candidate_transfers.loc[:, 'destination_idx'] = \
            candidate_transfers['destination_y'] \
            .apply(lambda destinations: destinations.index(destination))
    candidate_transfers['arrival_destination'] = candidate_transfers.apply(
        lambda tp: tp['arrival_y'][tp['destination_idx']], axis=1)
    candidate_transfers['arrival_next_stop'] = candidate_transfers['arrival_y'] \
        .apply(lambda arrival_lst: arrival_lst[0])
    candidate_transfers['cancellation_inbound'] = candidate_transfers.apply(
        lambda tp: tp['cancellation_x'][-1] != 0 \
                   | tp['cancellation_x'][tp['origin_idx']] != 0,
        axis=1
    )
    candidate_transfers['cancellation_outbound'] = candidate_transfers.apply(
        lambda tp: tp['cancellation_y'][tp['destination_idx']] != 0,
        axis=1
    )
    return candidate_transfers

def reachable_transfers(incoming_from_origin, outgoing, origin, destination, max_delay=60,
                        gains={}, max_hours=4, estimated_gain=0.0, worst_case=False, debug=False):
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
    max_delay_minutes = max_hours * 60
    delay = {'switch time': [], 'date': [], 'delay': [], 'reachable': []}
    outgoing_to_dest = outgoing[
            outgoing['destination']
            .apply(lambda destinations: destination in destinations)]
    # Train pairs where one train goes from origin to Frankfurt
    # and the other train (possible the same train)
    # goes from Frankfurt to destination
    candidate_transfers = incoming_from_origin.merge(outgoing_to_dest,
                                                     how='outer',
                                                     on='date')
    candidate_transfers.dropna(subset='origin_idx', inplace=True)
    candidate_transfers['origin_idx'] = candidate_transfers['origin_idx'].astype(int)
    # Time between the arrival of the first train at Frankfurt and
    # the departure of the second train
    candidate_transfers['transfer_time'] = \
        (candidate_transfers['departure_y']
         - candidate_transfers['arrival_x']).dt.total_seconds() / 60
    if debug:
        print(len(incoming_from_origin), len(candidate_transfers['in_id_x'].unique()))
    # Filter out trains that go from origin to destination directly,
    # as there is no train transfer in that case.
    candidate_transfers = candidate_transfers[
            candidate_transfers['in_id_x'] != candidate_transfers['in_id_y']]
    # Filter out trains that one can not transfer to, because they departed
    # before the incoming train arrived, or because it would take too long
    # (Restriction: may not take longer than max_hours)
    candidate_transfers = candidate_transfers[
            (candidate_transfers['transfer_time'] > 0) &
            (candidate_transfers['transfer_time'] <= max_delay_minutes)]
    if candidate_transfers.empty:
        return delay
    candidate_transfers = add_columns(candidate_transfers, destination)
    num_discarded = 0
    unique_ids = candidate_transfers['in_id_x'].unique()
    # Counters for a quick sanity check. Not used further in the analysis.
    num_found_alternative_to_frankfurt = 0
    num_not_found_alternative_to_frankfurt = 0
    num_found_alternative_from_frankfurt = 0
    num_not_found_alternative_from_frankfurt = 0
    for incoming_train_id in set(unique_ids):
        group_id = candidate_transfers[
                candidate_transfers['in_id_x'] == incoming_train_id]
        example_train = group_id.iloc[0]
        group_date = candidate_transfers[
                candidate_transfers['date'] == example_train['date']]
        for train in group_id.itertuples():
            # filter out trains for which we can't find next trains as we merge on the date
            # or train.departure_y.time() > threshold_time
            if train.transfer_time > max_delay:
                num_discarded += 1
                continue
            dest_idx = train.destination_idx
            plan_arrival = train.arrival_y[dest_idx]
            arrival_FRA = train.arrival_fra
            plan_departure_origin = train.departure_origin
            plan_difference, delay_difference = \
                get_plan_and_delay_difference(train, gains, estimated_gain, worst_case)

            if train.cancellation_inbound:
                # If the train that should arrive in Frankfurt was cancelled
                # Find the next train going from origin to Frankfurt as alternative
                train_reachable = 1
                # filtering so these trains have a planned departure
                # at the origin after the original train
                candidate_connections_to_frankfurt = group_date[
                        group_date['departure_origin'] > plan_departure_origin]
                next_train, extra_delay, dest_idx = \
                    find_next_train(train,
                                    candidate_connections_to_frankfurt,
                                    gains,
                                    estimated_gain,
                                    worst_case)
                if next_train is not None:
                    num_found_alternative_to_frankfurt += 1
                    train_delay = next_train.delay_y[dest_idx] + extra_delay
                else:
                    num_not_found_alternative_to_frankfurt += 1
                    train_delay = max_delay_minutes - train.transfer_time
            elif train.cancellation_outbound or plan_difference <= delay_difference:
                # If the departing train was cancelled or transfer to it is impossible
                train_reachable = 2
                # only look at trains that leave later in Frankfurt
                candidate_departing_trains = group_id[group_id['departure_y'] > arrival_FRA]
                next_train, extra_delay, dest_idx = \
                    find_next_train(train,
                                    candidate_departing_trains,
                                    gains,
                                    estimated_gain,
                                    worst_case)
                if next_train is not None:
                    num_found_alternative_from_frankfurt += 1
                    train_delay = next_train.delay_y[dest_idx] + extra_delay
                else:
                    if train.arrival_x.time() > time(24 - max_hours, 0, 0):
                        df1 = incoming_from_origin[incoming_from_origin['in_id'] == train.in_id_x]
                        df2 = outgoing_to_dest[outgoing_to_dest['date'] == train.date + timedelta(days=1)]
                        candidate_transfers_next_day = df1.merge(df2, how='cross')
                        if not candidate_transfers_next_day.empty:
                            candidate_transfers_next_day = add_columns(candidate_transfers_next_day, destination)
                            candidate_transfers_next_day['transfer_time'] = \
                                (candidate_transfers_next_day['departure_y']
                                 - candidate_transfers_next_day['arrival_x']).dt.total_seconds() / 60
                            candidate_transfers_next_day = candidate_transfers_next_day[
                                candidate_transfers_next_day['transfer_time'] <= max_delay_minutes]
                            next_train, extra_delay, dest_idx = find_next_train(train, candidate_transfers_next_day, gains, estimated_gain, worst_case)
                            if next_train is not None:
                                num_found_alternative_from_frankfurt += 1
                                train_delay = next_train.delay_y[dest_idx] + extra_delay
                            else:
                                num_not_found_alternative_from_frankfurt += 1
                                train_delay = max_delay_minutes - train.transfer_time
                        else:
                            num_not_found_alternative_from_frankfurt += 1
                            train_delay = max_delay_minutes - train.transfer_time
                    else:
                        num_not_found_alternative_from_frankfurt += 1
                        train_delay = max_delay_minutes - train.transfer_time
            else:
                # If it was possible to take the connecting train as planned
                train_reachable = 3
                train_delay = train.delay_y[dest_idx]
            if train_delay >= 0:
                delay['switch time'].append(plan_difference)
                delay['date'].append(plan_arrival.strftime('%Y-%m-%d %H:%M:%S'))
                delay['reachable'].append(train_reachable)
                delay['delay'].append(train_delay)
    if debug and len(candidate_transfers) > 0:
        print(num_discarded / len(candidate_transfers))
    if debug and num_found_alternative_to_frankfurt > 0 \
            and num_found_alternative_from_frankfurt > 0:
        print(num_found_alternative_to_frankfurt,
              num_not_found_alternative_to_frankfurt,
              num_found_alternative_from_frankfurt,
              num_not_found_alternative_from_frankfurt)
    return delay
