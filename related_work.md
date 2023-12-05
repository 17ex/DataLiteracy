# Related work

## General
### DB analysis
DB does a very basic monthly analysis of percantage of 5 min/15 min punctual trains for all, long distance and regional trains. (https://www.deutschebahn.com/de/konzern/konzernprofil/zahlen\_fakten/puenktlichkeitswerte-6878476)
I found no other DB analysis on their website.
They do however have DB analyitcs, but I could not find any published results yet:
"Verspätungsanalyse
Den Ursachen der Unpünktlichkeit auf der Spur
Auf der Basis einiger Milliarden Datensätze über den Betriebsablauf aller Zugfahrten in den letzten 5 Jahren haben wir die Entstehung von Verspätung und Unpünktlichkeit untersucht. Hierbei kamen vielfältige Big-Data-Technologien und statistische Algorithmen zum Einsatz. Durch die Integration einer großen Bandbreite von weiteren Datenquellen über Infrastruktur- und Fahrzeugzustand, Wetter, Reisendenströme etc. konnte die Aussagekraft der Analysen stark verbessert werden. Mit einer Musteranalyse von minimalen Planabweichungen im Sekundenbereich konnten verursachende Faktoren identifiziert werden. Hier konnten Maßnahmen zur Bemessung von Fahrzeitzuschlägen abgeleitet werden. Ein weiterer Treiber ist die deutliche Verkehrszunahme auf dem Schienennetz, die aufgrund der erhöhten Dichte der Streckenbelegung die Wahrscheinlichkeit von Verspätungsübertragungen zwischen den Zügen erhöht. Analysiert wurden auch die Auswirkungen des Baugeschehens auf die Pünktlichkeit mit dem Ziel, Baumaßnahmen zeitlich und räumlich so einzuplanen, dass sie möglichst geringe betriebliche Auswirkungen haben." (https://dbanalytics.deutschebahn.com/db-analytics-de/thema-c)

### David Kriesel
- Punctuality by train station
- Addition of delays by train station
- delay and cancellations by train (EC, IC, ICE)
- Delays, cancellations by day
- cancellations by train by week
- punctuality by time since start
- cancellations by daytime by station
- cancellations by first/middle/last stops
(https://www.youtube.com/watch?v=0rb9CfOvojk&t=3035s)

"Selten fällt ein Zug auf einer kompletten Strecke aus, manchmal muss ein Zug
jedoch vorzeitig umkehren." (https://www.bahn.de/service/ueber-uns/inside-bahn/hintergrund-technik/infografik-
warum-zuege-manchmal-ausfallen)

### Other scources
I did not find any other sources for basic analysis of DB data apart from many news articles including punctuality percentages.

## Delay Management 
"The question of delay management [DM] is whether passenger trains should wait
for delayed feeder trains or should depart on time" (Dollevoet et al., 2015)

Two different approches: train vs passenger oriented DM (König, 2020). DM normally minimizes passenger delays (König, 2020, Schöbel, 2009, ). DB implicitly states that they do train delay minimization (DB pdf). They do however account for passengers:

"Die Entscheidung, ob ein Anschlusszug wartet, wird immer im Einzelfall von speziellen Mitarbeiter:innen der Deutschen Bahn, den sogenannten Disponent:innen, getroffen. Sie ist abhängig von: 
- der Anzahl der Reisenden, die es betrifft 
- dem Angebot für Alternativverbindungen für die Reisenden 
- der Auswirkung der längeren Standzeit auf den wartenden Zug 
- der Belegungssituation der Gleise im Bahnhof und auf der Strecke

Wären also durch das Warten eines Zuges mehr Reisende von Verspätungen betroffen und gibt es noch dazu gute Alternativverbindungen, weil der Bahnhof einen Knotenpunkt darstellt, wird der Anschlusszug nicht warten, um insgesamt möglichst wenig Verspätungen zuverursachen." (https://www.bahn.de/service/ueber-uns/inside-bahn/hintergrund-technik/anschlusszug-wartet-nicht)

TODO look at the papers on more detail

## Other
"Um die daraus resultierenden Verspätungen
zu verdecken, werden diese in Gestalt verlängerter Fahrtzeiten in die Ankunfts-
und Abfahrtspläne eingearbeitet – mit dem Ergebnis, dass die DB AG nun auf
manchen Strecken wieder so lange braucht wie vor 60 Jahren" (Schöbel, 2009).

"Ob ein Ersatzzug eingesetzt werden soll, entscheiden im Einzelfall die Disponent:innen. Hier spielt es neben der Verspätungsdauer auch eine große Rolle, ob für die Reisenden schon Alternativen vorhanden sind oder ein geeignetes Ersatzfahrzeug sowie Personal zur Verfügung stehen. Ab 60 Minuten Verspätungsdauer wird in den meisten Fällen ein Ersatzzug für die Reisenden bereitgestellt. 
(what does mostly mean here, are there statistics on that?) 
Um im Einsatzfall schnell reagieren zu können, hat die Deutsche Bahn an neun Standorten bundesweit Einsatzreserven eingerichtet. Diese Standorte sind so verteilt, dass der Ersatzzug binnen 30 Minuten abfahrtbereit am Bahnsteig steht. 90\% des Fernverkehrsnetzes können dann innerhalb von weiteren 90 Minuten erreicht werden." (https://www.bahn.de/service/ueber-uns/inside-bahn/hintergrund-technik/infografik-warum-zuege-manchmal-ausfallen)

Only for regional trains, but passengers can ask connecting trains to wait for them. (https://www.zeit.de/news/2023-01/19/reisende-koennen-anschlusszug-per-app-zum-warten-auffordern)

Für die beste Entscheidung
11/2018 – Das Projekt KIRA ermöglicht schnellere Reaktionszeiten für Disponenten mithilfe einer neuen Art der Softwareentwicklung und zeigt damit schnelle und wertvolle Erfolge.
Die Arbeit der Disponenten bei DB Fernverkehr ist anspruchsvoll: im Problemfall den Überblick behalten, bei Zugausfällen oder Verspätungen Lösungen für die Reisenden finden und umsetzen – damit DB-Kunden optimal weiterreisen können. Das geht am besten mit fundierten Kenntnissen über die Situation der Reisenden im Zug. Wie das Projekt KIRA die notwendigen Informationen jetzt auf Grundlage einer neuartigen Software den Disponenten zur Verfügung stellt, verraten Dr. Felix-Sebastian Scholzen, fachlicher Projektleiter von DB Fernverkehr, Dr. Kai-Uwe Götzelt, technischer Projektleiter von DB Fernverkehr, sowie Wolfgang Harbach, Leiter des Solutions Center der DB Systel, jetzt im Interview mit digital spirit.
Was verbirgt sich hinter dem Projektnamen KIRA?
FELIX-SEBASTIAN SCHOLZEN: Ein überaus vielversprechendes Projekt, die Disposition bei DB Fernverkehr optimal weiterzuentwickeln. KIRA steht für „Kunden informieren und Reiseketten absichern“. Es will den Disponenten wichtige Informationen über die Reisenden im Zug liefern, die heute nur schwer zu beschaffen sind: Wie voll ist der Zug, welche Fahrgäste sind betroffen, woher kommen sie, wohin wollen sie? Diese Fakten liefert KIRA schnell und einfach, damit der Disponent die beste Entscheidung im Rahmen des betrieblich Machbaren treffen kann.
Woher kommen die Informationen?
FELIX-SEBASTIAN SCHOLZEN: Die Daten werden auf Basis der gekauften Tickets generiert. Und es gibt eine modellierte Komponente, um abzuschätzen, wie viele Zeitkarten-Besitzer oder Besitzer von Rail&Fly Tickets sich im Zug befinden. Dazu stehen uns Modelle aus Befragungen zur Verfügung. (https://www.dbsystel.de/dbsystel/ueber-uns/Digital-Stories/Fuer-die-beste-Entscheidung-6165980)

The DB introduced an additional 15 minute tolerance punctuality definition :D (https://www.deutschebahn.com/de/konzern/konzernprofil/zahlen\_fakten/puenktlichkeitswerte-6878476)

"Längere Umsteigezeiten werden laut [Vorstandsmitglied Michael] Peterson für etwa 800 Anschlussverbindungen hinterlegt. Je nach Verbindung könnten dies beispielsweise statt bislang 8 Minuten nun 10, 12 oder 14 Minuten sein, erklärte Peterson. Im Ergebnis kämen Fahrgäste 10, 20 oder 30 Minuten später am Ziel an. Fahrgäste können individuell auch kürzere oder längere Umsteigezeiten im System einstellen." (https://rp-online.de/wirtschaft/bahn-plant-laengere-umsteigezeit-fuer-zuverlaessigere-verbindungen_aid-74781029)

From DB data, 7% had to be removed to allow for analysis due to missing or inconsitent data. (Hauck et al.) They created a dataset which seems to be not published and never used in other papers.