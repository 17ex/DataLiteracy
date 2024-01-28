import pandas as pd


def find_gains_per_next_stop(incoming, outgoing):
    """
    Finds the biggest gain per next stop based on incoming and outgoing train data.
    A gain is positive if the train made up time on the way and negative
    if it lost time (delay at next station is bigger than at the previous)

    Args:
    - incoming (DataFrame): DataFrame containing incoming train information.
    - outgoing (DataFrame): DataFrame containing outgoing train information.

    Returns:
    - gains (dict): Dictionary containing all the gains in a list for each stop
    """
    gains = {}
    train_pairs = pd.merge(incoming, outgoing, on='in_id', how='inner')
    for train_pair in train_pairs.itertuples():
        # Extracting relevant information from the merged DataFrame
        departure = train_pair.departure_y
        arrival = train_pair.arrival_x
        delay_in = train_pair.delay_x
        delay_out = train_pair.delay_y[0]
        destination = train_pair.destination_y[0]

        # Skip data point if either of the two trains had a canceled stop
        # as we can't obtain useful information from them
        if (
                1 in train_pair.cancellation_x or
                2 in train_pair.cancellation_x or
                1 in train_pair.cancellation_y or
                2 in train_pair.cancellation_y
           ):
            continue

        driving_time = (train_pair.arrival_y[0] - departure).total_seconds() / 60
        planned_transfer_time = (departure - arrival).total_seconds() / 60
        departure_delay = max(0, delay_in - planned_transfer_time)
        gain = departure_delay - delay_out

        # Handling cases where gain exceeds a threshold of 27% of the time the train takes
        if gain > 0.27 * driving_time:
            continue
        if destination not in gains.keys():
            gains[destination] = [gain]
        else:
            gains[destination].append(gain)

    return gains


def get_plan_and_delay_difference(train_pair, gains={}, estimated_gain=0.0, worst_case=False):
    """
    For a given train pair, estimates the time difference between departure
    of the second train and arrival of the first according to the schedule,
    as well as the difference between the delay of arrival of the first train
    and an estimation of its departure delay (as the departure delays are
    not in the dataset).
    If worst_case=True, it is assumed that the second train departed as planned,
    and its delay at the next stop was due to something that occurred
    on the way to its next stop.
    Otherwise, it is taken into account how much delay the train could make up
    for on the way to the next station (given in gains) in order to estimate
    its departure delay

    Args:
    - train_pair (row of a DataFrame): DataFrame containing information
        about the trains.
    - gains (dict, optional): Dictionary containing gains for every stop.
        Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Only used as an
        alternative for when gains is not specified.
        Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario.
        Default is False.

    Returns:
    - plan_difference (float): Time difference between departure of the
        second train and arrival of the first train
    - delay_difference (float): Difference between the arrival delay of the
        first train and the departure delay of the second train
    """
    arrival_FRA = train_pair.arrival_x
    departure_FRA = train_pair.departure_y
    in_delay = train_pair.delay_x
    next_stop = train_pair.destination_y[0]
    delay_at_next_stop = train_pair.delay_y[0]
    arrival_at_next_stop = train_pair.arrival_next_stop
    plan_difference = (departure_FRA - arrival_FRA).total_seconds() / 60

    if worst_case:
        out_delay = 0
    elif gains:
        if next_stop in gains.keys():
            potential_gain = gains[next_stop]
        else:
            potential_gain = 0
        out_delay = max(0, delay_at_next_stop + potential_gain)
    else:
        gain = estimated_gain * (arrival_at_next_stop - departure_FRA).total_seconds() / 60
        out_delay = max(0, delay_at_next_stop + gain)
    return plan_difference, max(0, in_delay - out_delay)


def can_take_connecting_train(train_pair, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Estimates whether the connecting train can be taken, based on an estimation of
    its actual departure, based on its planned departure. If worst_case=False,
    it is also taken into account how late the departing train
    was at its next stop, and an estimation of how long it could have additionally
    waited at Frankfurt, based on how much of the additional delay could
    be remedied before the next stop (specified in gains).
    In the worst-case scenario, it is assumed that the connecting train
    departed as planned (without delay).

    Args:
    - train_pair (row of a DataFrame): DataFrame containing information about the trains.
    - gains (dict, optional): Dictionary containing gains for every stop. Default is an empty dictionary.
    - estimated_gain (float, optional): Estimated gain. Default is 0.0.
    - worst_case (bool, optional): Flag for worst-case scenario. Default is False.

    Returns:
    - bool: if true, according to the model, the connecting train can be taken.
    """
    plan_difference, delay_difference = get_plan_and_delay_difference(
            train_pair, gains, estimated_gain, worst_case
            )
    return plan_difference > delay_difference


def get_directions():
    """
    These are 5 manually defined rough directions for the train stations
    in the data set, relative to Frankfurt Hbf.

    Args:
    none

    Returns:
    - directions (dict): Contains the 5 directions as keys and lists containing
        the corresponding station names as values
    """
    # TODO
    # Save this in a text file, move this function that loads it to data_tools
    directions = {
        'South': ['Weinheim(Bergstr)Hbf', 'Bruchsal', 'Karlsruhe-Durlach',
                  'Günzburg', 'Bensheim', 'Mannheim Hbf', 'Stuttgart Hbf',
                  'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                  'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf',
                  'Wiesloch-Walldorf', 'Offenburg', 'Freiburg(Breisgau) Hbf'],
        'West': ['Hamm(Westf)Hbf', 'Aachen Hbf', 'Mönchengladbach Hbf',
                 'Siegburg/Bonn', 'Hagen Hbf', 'Duisburg Hbf',
                 'Recklinghausen Hbf', 'Andernach', 'Köln/Bonn Flughafen',
                 'Solingen Hbf', 'Oberhausen Hbf', 'Montabaur',
                 'Münster(Westf)Hbf', 'Bochum Hbf', 'Wuppertal Hbf',
                 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                 'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf',
                 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf',
                 'Gelsenkirchen Hbf', 'Essen Hbf'],
        'North': ['Kassel-Wilhelmshöhe', 'Lüneburg', 'Göttingen',
                  'Hannover Messe/Laatzen', 'Uelzen', 'Hannover Hbf',
                  'Celle', 'Hamburg Dammtor', 'Neumünster', 'Treysa',
                  'Marburg(Lahn)', 'Gießen', 'Friedberg(Hess)',
                  'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kiel Hbf'],
        'North East': ['Weißenfels', 'Wittenberge', 'Naumburg(Saale)Hbf',
                       'Stendal Hbf', 'Halle(Saale)Hbf', 'Bitterfeld',
                       'Berlin Ostbahnhof', 'Berlin Südkreuz',
                       'Dresden-Neustadt', 'Wolfsburg Hbf', 'Eisenach',
                       'Dresden Hbf', 'Berlin-Spandau',
                       'Lutherstadt Wittenberg Hbf', 'Riesa', 'Hildesheim Hbf',
                       'Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf',
                       'Leipzig Hbf', 'Brandenburg Hbf', 'Magdeburg Hbf',
                       'Berlin Gesundbrunnen'],
        'East': ['München-Pasing', 'München Hbf', 'Augsburg Hbf', 'Plattling',
                 'Aschaffenburg Hbf', 'Passau Hbf', 'Nürnberg Hbf',
                 'Würzburg Hbf', 'Regensburg Hbf', 'Ingolstadt Hbf']}
    return directions
