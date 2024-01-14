import pandas as pd


def find_gains_per_next_stop(incoming, outgoing):
    """
    Finds the biggest gain per next stop based on incoming and outgoing train data.
    A gain is positive if the train was faster than it was planned and negative if it was slower.

    Args:
    - incoming (DataFrame): DataFrame containing incoming train information.
    - outgoing (DataFrame): DataFrame containing outgoing train information.

    Returns:
    - all_gains (dict): Dictionary containing all the gains in a list for each stop.
    """
    all_gains = {} # Dictionary to store all gains per next stop
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

        if destination not in all_gains.keys():
            all_gains[destination] = [gain]
        else:
            all_gains[destination].append(gain)

    return all_gains


def reachable_train(train, gains={}, estimated_gain=0.0, worst_case=False):
    """
    Calculates the plan difference and delay difference for a given train.

    Args:
    - train (row of a DataFrame): DataFrame containing information about the train.
    - gains (dict, optional): Dictionary containing gains for every stop. Default is an empty dictionary.
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
        if destination in gains.keys():
            gain = gains[destination]
        else:
            gain = 0
        out_delay = max(0, dest_delay[0] + gain)
        delay_difference = max(0, in_delay - out_delay)
    else:
        estimated_gain * (dest_arrival - departure_FRA).total_seconds() / 60
        out_delay = max(0, dest_delay[0] + estimated_gain)
        delay_difference = max(0, in_delay - out_delay)
    return plan_difference, delay_difference


def get_directions():
    directions = {
        'South': ['Weinheim(Bergstr)Hbf', 'Bruchsal', 'Karlsruhe-Durlach', 'Günzburg', 'Bensheim', 'Mannheim Hbf',
                  'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                  'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf', 'Offenburg',
                  'Freiburg(Breisgau) Hbf'],
        'West': ['Hamm(Westf)Hbf', 'Aachen Hbf', 'Mönchengladbach Hbf', 'Siegburg/Bonn', 'Hagen Hbf', 'Duisburg Hbf',
                 'Recklinghausen Hbf', 'Andernach', 'Köln/Bonn Flughafen', 'Solingen Hbf', 'Oberhausen Hbf',
                 'Montabaur', 'Münster(Westf)Hbf', 'Bochum Hbf', 'Wuppertal Hbf', 'Köln Hbf', 'Mainz Hbf',
                 'Frankfurt(Main)West',
                 'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf',
                 'Gelsenkirchen Hbf', 'Essen Hbf'],
        'North': ['Kassel-Wilhelmshöhe', 'Lüneburg', 'Göttingen', 'Hannover Messe/Laatzen', 'Uelzen', 'Hannover Hbf',
                  'Celle', 'Hamburg Dammtor', 'Neumünster', 'Treysa', 'Marburg(Lahn)', 'Gießen', 'Friedberg(Hess)',
                  'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kiel Hbf'],
        'North East': ['Weißenfels', 'Wittenberge', 'Naumburg(Saale)Hbf', 'Stendal Hbf', 'Halle(Saale)Hbf',
                       'Bitterfeld', 'Berlin Ostbahnhof', 'Berlin Südkreuz', 'Dresden-Neustadt', 'Wolfsburg Hbf',
                       'Eisenach', 'Dresden Hbf', 'Berlin-Spandau', 'Lutherstadt Wittenberg Hbf', 'Riesa',
                       'Hildesheim Hbf', 'Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf',
                       'Brandenburg Hbf', 'Magdeburg Hbf', 'Berlin Gesundbrunnen'],
        'East': ['München-Pasing', 'München Hbf', 'Augsburg Hbf', 'Plattling', 'Aschaffenburg Hbf', 'Passau Hbf',
                 'Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Ingolstadt Hbf']}
    return directions
