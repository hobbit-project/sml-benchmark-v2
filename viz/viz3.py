import math
import os

import matplotlib
#import utm
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt


from Supplementary import Trip, Ship, Port, Point

ports = None
# with open("../data/ports.csv") as f:
with open("../data/ports.csv") as f:
    portsLines = f.readlines()

ports={}
for line in portsLines[1::]:
    splitted = line.split(",")
    port = Port(splitted[0], float(splitted[1]), float(splitted[2]), float(splitted[3]))
    ports[port.name] = port

content = None
filename = "test_composed_labeled_v8_nodupl_reduced_splitted"

path = "../data/debs2018_"+filename+".csv"


dirToWrite="pics_"+filename

if not os.path.exists(dirToWrite):
    os.mkdir(dirToWrite)
if not os.path.exists(dirToWrite+"/labeled"):
    os.mkdir(dirToWrite+"/labeled")
if not os.path.exists(dirToWrite+ "/notLabeled"):
    os.mkdir(dirToWrite + "/notLabeled")

with open(path) as f:
    content = f.readlines()

lons = []
lats = []


tripsPerShip = {}
shipIds = []

for line in content[1::]:
    splitted = line.split(",")
    shipId = splitted[0]

    point = Point(splitted)

    if not shipId in shipIds:
        shipIds.append(shipId)
        ship = Ship()
    else:
        ship = tripsPerShip[shipId]

    trip = None
    if not point.tripId in ship.labeledTripIds and not point.tripId in ship.notLabeledTripIds:
        trip = Trip()
        if point.arrival_calc is not None and point.arrival_calc!="":
            ship.labeledTripIds.append(point.tripId)
        else:
            ship.notLabeledTripIds.append(point.tripId)
    else:
        trip = ship.trips[point.tripId]

    if point.timestamp == point.arrival_calc:
        trip.labelPoint = point

    trip.points.append(point)
    trip.lons.append(point.lon)
    trip.lats.append(point.lat)

    ship.minLat = min(ship.minLat, point.lat)
    ship.maxLat = max(ship.maxLat, point.lat)

    ship.minLon = min(ship.minLon, point.lon)
    ship.maxLon = max(ship.maxLon, point.lon)

    ship.trips[point.tripId] = trip
    tripsPerShip[shipId] = ship

print "Trips readings were finished";
#m = Basemap(resolution='h',projection=mp,lon_0=0)
#m = Basemap(resolution='h',projection=mp,lon_0=0,area_thresh=200000)
#m = Basemap(resolution=None,projection=mp,lon_0=minLon, lon_1=maxLon, lat_0=minLat, lat_1=maxLat)


plt.rcParams.update({'font.size': 7})
plt.rcParams['figure.figsize']=(20,20)


colors = ['r','g','b','c','m','w']
shipIndex=0


def preparePicture(ship, ports, tripIds, dirToWrite):
    m = Basemap(projection='lcc', resolution='c', lon_0=ship.minLon, lat_0=ship.minLat, lat_ts=ship.maxLon, \
                llcrnrlat=ship.minLat - 3, urcrnrlat=ship.maxLat + 3, \
                llcrnrlon=ship.minLon - 3, urcrnrlon=ship.maxLon + 5, area_thresh=20000000
                )

    xmin, ymin = m(ship.minLon, ship.minLat)
    xmax, ymax = m(ship.maxLon, ship.maxLat)

    ax = plt.gca()

    ax.set_xlim([xmin, xmax])
    ax.set_ylim([ymin, ymax])

    letfTopCorner = m(ship.minLon - 9, ship.maxLat)

    for portName in ports.keys()[:]:
        port = ports[portName]
        m.plot(port.lons, port.lats, 'k-', label=portName, latlon=True, linewidth=0.5, color="k")

        if port.lon >= ship.minLon - 3 and port.lon <= ship.maxLon + 3 and port.lat >= ship.minLat - 3 and port.lat <= ship.maxLat + 3:
            point = m(port.lon, port.lat)
            plt.text(point[0], point[1], portName)

    colorIndex = 0
    index = 0
    for tripId in tripIds[::]:
        trip = ship.trips[tripId]
        index += 1
        m.plot(trip.lons, trip.lats, 'k-', latlon=True, linewidth=1, color=colors[colorIndex])
        m.plot(trip.lons, trip.lats, 'bo', latlon=True, markersize=1, color=colors[colorIndex])

        tripLegend = str(index) + ". " + trip.points[0].tripId
        if trip.labelPoint is not None:
            m.plot([trip.labelPoint.lon], [trip.labelPoint.lat], 'bo', latlon=True, markersize=4, color=colors[colorIndex])
            tripLegend += " Label: "+trip.labelPoint.arrival_calc+" "+trip.labelPoint.arrival_port_calc;
            #plt.text(trip.labelPoint.lon, trip.labelPoint.lat + 20000, trip.labelPoint.arrival_calc+" "+trip.labelPoint.arrival_port_calc, color=colors[colorIndex])

        startPoint = m(trip.lons[0], trip.lats[0])
        plt.text(startPoint[0], startPoint[1] + 20000, str(index), color=colors[colorIndex])

        parking = "st"
        if "parking" in trip.points[0].tripId:
            parking = "park"
        plt.text(10000, letfTopCorner[1] - 60000 * index, tripLegend, color=colors[colorIndex])

        colorIndex += 1
        if colorIndex == len(colors):
            colorIndex = 0

    m.shadedrelief()

    plt.savefig(dirToWrite + "/" + shipId + ".png")
    # plt.show()
    plt.close()

for shipId in shipIds[::]:
    ship = tripsPerShip[shipId]
    print(str(shipIndex)+". "+shipId)

    #fig, ax = plt.subplots()
    if len(ship.labeledTripIds)>0:
        preparePicture(ship, ports, ship.labeledTripIds, dirToWrite+"/labeled")
    if len(ship.notLabeledTripIds) > 0:
        preparePicture(ship, ports, ship.notLabeledTripIds, dirToWrite + "/notLabeled")


    shipIndex+=1