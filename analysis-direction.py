import numpy as np
from sklearn.tree import DecisionTreeRegressor, plot_tree
import pickle
import matplotlib.pyplot as plt

with open('data/incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open('data/outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)


directions = {'South': ['Weinheim(Bergstr)Hbf', 'Bruchsal', 'Karlsruhe-Durlach', 'Günzburg', 'Bensheim', 'Mannheim Hbf', 'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                        'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf', 'Offenburg', 'Freiburg(Breisgau) Hbf'],
              'West': ['Hamm(Westf)Hbf', 'Aachen Hbf', 'Mönchengladbach Hbf', 'Siegburg/Bonn', 'Hagen Hbf', 'Duisburg Hbf', 'Recklinghausen Hbf', 'Andernach', 'Köln/Bonn Flughafen', 'Solingen Hbf', 'Oberhausen Hbf', 'Montabaur', 'Münster(Westf)Hbf', 'Bochum Hbf', 'Wuppertal Hbf', 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                       'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf', 'Gelsenkirchen Hbf', 'Essen Hbf'],
              'North': ['Kassel-Wilhelmshöhe', 'Lüneburg', 'Göttingen', 'Hannover Messe/Laatzen', 'Uelzen', 'Hannover Hbf', 'Celle', 'Hamburg Dammtor', 'Neumünster', 'Treysa', 'Marburg(Lahn)', 'Gießen', 'Friedberg(Hess)', 'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kiel Hbf'],
              'North East': ['Weißenfels', 'Wittenberge', 'Naumburg(Saale)Hbf', 'Stendal Hbf', 'Halle(Saale)Hbf', 'Bitterfeld', 'Berlin Ostbahnhof','Berlin Südkreuz', 'Dresden-Neustadt', 'Wolfsburg Hbf', 'Eisenach', 'Dresden Hbf', 'Berlin-Spandau', 'Lutherstadt Wittenberg Hbf', 'Riesa', 'Hildesheim Hbf', 'Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf',
                             'Brandenburg Hbf', 'Magdeburg Hbf', 'Berlin Gesundbrunnen'],
              'East': ['München-Pasing', 'München Hbf', 'Augsburg Hbf', 'Plattling', 'Aschaffenburg Hbf', 'Passau Hbf', 'Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Ingolstadt Hbf']}
train_directions = {'South': [], 'West': [], 'North': [], 'North East': [], 'East': []}

not_found = 0
found = 0
direction = []
weekday = []
year = []
hour = []
delay = []

not_found_stations = {""}
munich = 0
airport = 0
other = 0

for train_out in outgoing.itertuples():
    found_direction = False
    for dest in train_out.destination:
        if dest in directions['South']:
            found += 1
            found_direction = True
            train_directions['South'].append(train_out)
            direction.append(1)
            break
        elif dest in directions['West']:
            found += 1
            found_direction = True
            train_directions['West'].append(train_out)
            direction.append(2)
            break
        elif dest in directions['North']:
            found += 1
            found_direction = True
            train_directions['North'].append(train_out)
            direction.append(3)
            break
        elif dest in directions['North East']:
            found += 1
            found_direction = True
            train_directions['North East'].append(train_out)
            direction.append(4)
            break
        elif dest in directions['East']:
            found += 1
            found_direction = True
            train_directions['East'].append(train_out)
            direction.append(5)
            break
    if not found_direction:
        not_found += 1
        #print(train_out.destination)
        for dest in train_out.destination:
            not_found_stations.add(dest)
            if dest in ['München-Pasing', 'München Hbf', 'Augsburg Hbf']:
                munich += 1
                break
            elif dest in ['Frankfurt am Main Flughafen Fernbahnhof']:
                airport += 1
                break
            else:
                other += 1
                break
    else:
        delay.append(np.mean(train_out.delay))
        year.append(train_out.date.year - 2021)
        weekday.append(train_out.date.weekday())
        hour.append(train_out.departure.hour)

print(found)
print(not_found)
print(not_found_stations)
print("München")
print(munich)
print(airport)
print(other)

print(1, 'South:', len(train_directions['South']))
print(2, 'West:', len(train_directions['West']))
print(3, 'North:', len(train_directions['North']))
print(4, 'North East:', len(train_directions['North East']))
print(5, 'East:', len(train_directions['East']))

X = np.array([direction]).T  # , weekday, year, hour
y = np.array(delay)  # Target variable

model = DecisionTreeRegressor(random_state=42)
model.fit(X, y)

# Visualize the decision tree
plt.figure(figsize=(12, 8))
plot_tree(model, feature_names=['direction'], filled=True, fontsize=10)
plt.show()

# Get feature importances
feature_importances = model.feature_importances_

# Print feature importances
print("Feature Importances:", feature_importances)