import math

import numpy as np


class Port():
    name = None
    lon = None
    lat = None
    lons = None
    lats = None
    radius = None

    def __init__(self, name, lon, lat, radius):
        self.name = name
        self.lon = lon
        self.lat = lat
        self.radius = radius
        self.lons = []
        self.lats = []
        self.calculateCirclePoints()

    def calculateCirclePoints(self):
        coef = self.radius/80.0
        for angle in np.arange(0, 370, 10):
            x = self.lon + coef*math.cos(3.14*(angle/180.0))
            y = self.lat + coef*math.sin(3.14*(angle/180.0))
            self.lons.append(x)
            self.lats.append(y)

class Trip():
    lons = None
    lats = None
    points = None
    labelPoint = None


    def __init__(self):
        self.lons = []
        self.lats = []
        self.points = []
        self.labelPoint = None

    def getId(self):
        return self.points[0]["trip_id"]

class Point():
    lon = None
    lat = None
    timestamp = None
    tripId = None
    arrival_calc = None
    arrival_port_calc = None

    def __init__(self, splitted):

        self.lon = float(splitted[3])
        self.lat = float(splitted[4])
        self.timestamp = splitted[7]
        self.tripId = splitted[10]

        if len(splitted) == 13:
            self.arrival_calc = splitted[11]
            self.arrival_port_calc = splitted[12]

    def getId(self):
        return self.points[0]["trip_id"]

class Ship():

    trips = None
    labeledTripIds = None
    minLat = None
    maxLat = None

    minLon = None
    maxLon = None

    def __init__(self):
        self.trips = {}
        self.labeledTripIds = []
        self.notLabeledTripIds = []
        self.minLat = 99999
        self.maxLat = 0

        self.minLon = 99999
        self.maxLon = 0

