package org.hobbit.smlbenchmark_v2.utils;

import jnr.ffi.annotations.In;
import org.hobbit.smlbenchmark_v2.benchmark.generator.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;

import static org.hobbit.smlbenchmark_v2.Constants.CHARSET;

public class Dataset {
    public Logger logger = LoggerFactory.getLogger(Dataset.class);

    public Dataset(Logger logger){
        if(logger!=null)
            this.logger = logger;
    }


    public String[] readFile(Path filepath, int linesLimit) throws IOException {
        logger.debug("Reading "+filepath);

        //URL url  = Resources.getResource(filepath.toString());
        //List<String> lines = Resources.readLines(url,CHARSET);

        List<String> lines = Files.readAllLines(filepath, CHARSET);
        if(linesLimit>0 && lines.size()>linesLimit)
            lines = lines.subList(0, linesLimit+1);
        logger.debug("File reading finished ({})",lines.size());
        return lines.toArray(new String[0]);
    }

//    public Map<String, Map<String, Trip>> getTripsPerShips(String[] lines, int totalTuplesLimit) throws Exception {
//        Map<String, Map<String, Trip>> ret = new LinkedHashMap<>();
//
//        Map<String, Map<String, Trip>> allPoints = getTripsPerShips(lines, 0);
//
//        for(String shipId: allPoints.keySet()) {
//            Map<String, Trip> shipTrips = new LinkedHashMap<String, Trip>();
//            if (ret.containsKey(shipId))
//                shipTrips = ret.get(shipId);
//
//            ret.put(shipId, shipTrips);
//        }
//    }

    public Map<String, List<DataPoint>> getPointsPerShip(String[] lines) throws IOException, ParseException {
        //for multiple threads sending
        Map<String, List<DataPoint>> ret = new LinkedHashMap<>();

        Map<String, List<String>> destsPerShip = new HashMap<>();

        String headLine = lines[0].replace("\uFEFF","").toLowerCase();
        String[] separators = new String[]{ "\t", ";", "," };
        int sepIndex = 0;
        String[] splitted = headLine.split(separators[sepIndex]);
        while(splitted.length==1){
            sepIndex++;
            splitted = headLine.split(separators[sepIndex]);
        }

        List<String> headings = Arrays.asList(splitted);
        String separator = separators[sepIndex];

        int totalPoints=0;
        for(int i=1; i<lines.length; i++){
            try {
                DataPoint point = new DataPoint(lines[i], headings, separator);
                String shipId = point.getValue("ship_id").toString();

                List<DataPoint> shipPoints = new ArrayList<DataPoint>();
                if (ret.containsKey(shipId))
                    shipPoints = ret.get(shipId);
                totalPoints++;
                shipPoints.add(point);
                ret.put(shipId, shipPoints);
            }
            catch (Exception e){
                logger.error(e.getMessage());
            }

        }
        logger.debug("Processing finished: {} points total", totalPoints);
        return ret;
    }

    public Map<String, Map<String, Trip>> getTripsPerShips(String[] lines, int tuplesLimit) throws Exception {
        Map<String, Map<String, Trip>> readed = getTripsPerShips(lines);
        if (tuplesLimit==0 || tuplesLimit>=lines.length)
            return readed;
        return getTripsPerShips(readed, tuplesLimit);
    }

    public Map<String, Map<String, Trip>> getTripsPerShips(Map<String, Map<String, Trip>> tripsPerShips, int tuplesLimit) throws Exception {

        Map<String, Map<String, Trip>> ret = new HashMap<>();
        Map<String, Integer> shipTripIndexes = new HashMap<>();

        int totalPlacedPoints=0;
        int iterations=0;
        while (totalPlacedPoints<tuplesLimit){
            Map<String, Map<String, Trip>> cloned = new LinkedHashMap<>(tripsPerShips);
            for (String shipId : cloned.keySet()){
                Map<String, Trip> thisShipTrips = cloned.get(shipId);
                Map<String, Trip> thisShipTripsToReturn = (ret.containsKey(shipId) ? ret.get(shipId) : new LinkedHashMap<>());
                int index = (shipTripIndexes.containsKey(shipId)?shipTripIndexes.get(shipId): 0);
                String[] tripKeys = thisShipTrips.keySet().toArray(new String[0]);
                if(index<tripKeys.length){
                    Trip trip = thisShipTrips.get(tripKeys[index]);
                    thisShipTripsToReturn.put(trip.getId(), trip);
                    totalPlacedPoints+=trip.getPoints().size();
                    ret.put(shipId, thisShipTripsToReturn);
                    index++;
                    shipTripIndexes.put(shipId, index);
                }else
                    tripsPerShips.remove(shipId);
                if(totalPlacedPoints>=tuplesLimit)
                    break;

            }
            iterations++;
        }
        return ret;
    }

    public Map<String, Map<String, Trip>> getTripsPerShips(String[] lines) throws Exception {
        logger.debug("Processing {} lines", lines.length);

        List<DataPoint> ret = new ArrayList<>();

        String headLine = lines[0].replace("\uFEFF","").toLowerCase();
        String[] separators = new String[]{ "\t", ";", "," };
        int sepIndex = 0;
        String[] splitted = headLine.split(separators[sepIndex]);
        while(splitted.length==1){
            sepIndex++;
            splitted = headLine.split(separators[sepIndex]);
        }

        List<String> headings = Arrays.asList(splitted);
        String separator = separators[sepIndex];

        String prevLine=null;
        for(int i=1; i<lines.length; i++) {
            String line = lines[i];
            if (!line.equals(prevLine)) {
                DataPoint point = new DataPoint(line, headings, separator);
                ret.add(point);
            }
            prevLine = line;
        }
        return getTripsPerShips(ret);
    }

    public Map<String, Map<String, Trip>> getTripsPerShips(List<DataPoint> points) throws Exception {

        Map<String, Map<String, Trip>> ret = new LinkedHashMap<>();

        int tuplesCount=0;
        int labeledTuplesCount = 0;


        for(DataPoint point : points){

            String shipId = point.getValue("ship_id").toString();

            Map<String, Trip> shipTrips = new LinkedHashMap<String, Trip>();
            if (ret.containsKey(shipId))
                shipTrips = ret.get(shipId);

            String tripId = (point.containsKey("trip_id") ? point.get("trip_id") : "noId");

            if(point.containsKey("arrival_calc"))
                labeledTuplesCount++;

            Trip trip = null;
            if (shipTrips.containsKey(tripId))
                trip = shipTrips.get(tripId);
            else
                trip = new Trip(tripId.endsWith("parking") ? "parking" : null);

            trip.addPoint(point);
            shipTrips.put(tripId, trip);
            tuplesCount++;
            ret.put(shipId, shipTrips);

        }

        int tripsCount = ret.values().stream().map(shipTripsList->shipTripsList.size()).mapToInt(Integer::intValue).sum();
        long labeledTripsCount = ret.values().stream().map(shipTripsList->shipTripsList.values().stream().filter(trip->trip.getLabeledTuplesCount()>0)).count();
        logger.debug("Processing finished: {} tuples, {} labeled, {} ships, {} trips, {} labeled", tuplesCount, labeledTuplesCount, ret.size(), tripsCount, labeledTripsCount);
        return ret;
    }
}
