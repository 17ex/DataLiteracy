import numpy as np
from sklearn.tree import DecisionTreeRegressor, plot_tree
import pickle
import matplotlib.pyplot as plt

with open('data/incoming.pkl', 'rb') as file:
    incoming = pickle.load(file)

with open('data/outgoing.pkl', 'rb') as file:
    outgoing = pickle.load(file)

directions = {'South': ['Mannheim Hbf', 'Stuttgart Hbf', 'Karlsruhe Hbf', 'Kaiserslautern Hbf', 'Saarbrücken Hbf',
                        'Baden-Baden', 'Ulm Hbf', 'Heidelberg Hbf', 'Darmstadt Hbf', 'Wiesloch-Walldorf', 'Augsburg Hbf'],
              'West': ['Frankfurt am Main Flughafen Fernbahnhof', 'Köln Hbf', 'Mainz Hbf', 'Frankfurt(Main)West',
                       'Dortmund Hbf', 'Koblenz Hbf', 'Bonn Hbf', 'Köln Messe/Deutz', 'Düsseldorf Hbf', 'Wiesbaden Hbf'],
              'North': ['Hannover Hbf', 'Hamburg Hbf', 'Bremen Hbf', 'Hamburg-Altona', 'Kassel-Wilhelmshöhe', 'Kiel Hbf'],
              'North East': ['Berlin Hbf', 'Braunschweig Hbf', 'Erfurt Hbf', 'Leipzig Hbf',
                             'Brandenburg Hbf', 'Magdeburg Hbf', 'Berlin Gesundbrunnen', 'Bad Hersfeld', 'Fulda'],
              'East': ['Nürnberg Hbf', 'Würzburg Hbf', 'Regensburg Hbf', 'Hanau Hbf']}
train_directions = {'South': [], 'West': [], 'North': [], 'North East': [], 'East': []}

not_found = 0
found = 0
direction = []
weekday = []
year = []
hour = []
delay = []
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
    else:
        delay.append(np.mean(train_out.delay))
        year.append(train_out.date.year - 2021)
        weekday.append(train_out.date.weekday())
        hour.append(train_out.departure.hour)

print(outgoing)
print(found)
print(not_found)
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