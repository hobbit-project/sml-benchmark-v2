package org.hobbit.smlbenchmark_v2.utils;

import org.hobbit.smlbenchmark_v2.benchmark.generator.DataPoint;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hobbit.smlbenchmark_v2.Constants.DEFAULT_DATE_FORMAT;

public class Trip {
    private String id_appendix;
    private String departurePortName;
    private String arrivalTimestamp;
    private String arrivalPortName;
    private String sequence="";

    private List<DataPoint> points = new ArrayList<>();

    public Trip(){

    }

    public Trip(String id_appendix){
        if(id_appendix!=null)
            this.id_appendix = id_appendix;
    }

    public void addPoint(DataPoint tuple){
        points.add(tuple);
    }

    public void addAllPoints(List<DataPoint> tuples){
        this.points.addAll(tuples);
    }

    public String getId(){
        //return this.points.get(0).get("ship_id").substring(0,7)+"_"+this.points.get(0).get("timestamp").substring(0, this.points.get(0).get("timestamp").length() - 2) + "xx_"+this.points.get(0).get("departure_port_name")+(id_appendix!=null?"_"+id_appendix:"");
        //return this.points.get(0).get("ship_id").substring(0,7)+"_"+this.points.get(0).get("timestamp").substring(0, this.points.get(0).get("timestamp").length() - 2) + "xx - "+this.points.get(this.points.size()-1).get("timestamp")+(id_appendix!=null?"_"+id_appendix:"");
        return this.points.get(0).get("ship_id").substring(0,7)+"_"+this.points.get(0).get("timestamp").substring(0, this.points.get(0).get("timestamp").length() - 0) + " - "+this.points.get(this.points.size()-1).get("timestamp")+(id_appendix!=null?"_"+id_appendix:"");
    }

    public List<DataPoint> getPoints(){
        return this.points;
    }

    public int getLabeledTuplesCount(){

        long ret = this.points.stream().filter(tuple->tuple.containsKey("arrival_calc")).count();
        return new Long(ret).intValue();
    }

    public void setArrivalTimestamp(String timestamp){
        this.arrivalTimestamp = timestamp;
    }

    public void setArrivalPortName(String portName){
        this.arrivalPortName = portName;
    }

    public String getDeparturePortName(){
        return this.departurePortName;
    }

    public void setDeparturePortName(String portName){
        this.departurePortName = portName;
    }

    public long getDuration() throws ParseException {
        long duration = 0;
        try {
            long initTupleTime = DEFAULT_DATE_FORMAT.parse(points.get(0).get("timestamp")).getTime();
            long lastTupleTime = DEFAULT_DATE_FORMAT.parse(points.get(points.size() - 1).get("timestamp")).getTime();
            duration = lastTupleTime - initTupleTime;
        }
        catch (Exception e){
            String test="123";
        }
        if(duration==0)
            duration=1;

        return duration;
    }

    public long getLabeledDuration() throws ParseException {
        if (arrivalTimestamp==null)
            return 0;
        long initTupleTime = DEFAULT_DATE_FORMAT.parse(points.get(0).get("timestamp")).getTime();
        long lastTupleTime = DEFAULT_DATE_FORMAT.parse(arrivalTimestamp).getTime();
        return lastTupleTime-initTupleTime;
    }


    public List<String> toRows(){
        List<String> ret = new ArrayList<>();

        Boolean labelTuples = (arrivalTimestamp != null && arrivalPortName != null? true: false);
        for (DataPoint point : getPoints()) {

            List<String> strs = new ArrayList<>();
            if(departurePortName!=null && !departurePortName.equals(point.get("departure_port_name"))){
                point.setValue("departure_port_name", departurePortName);
                String[] splitted = point.get("raw").split(",");
                splitted[8] = departurePortName;
                strs.add(String.join(",", splitted));
            }else
                strs.add(point.get("raw"));
            strs.add(getId());

            if(labelTuples){
                    strs.add(arrivalTimestamp);
                    strs.add(arrivalPortName);
            }else{
                if(point.containsKey("arrival_calc"))
                    strs.add(point.get("arrival_calc"));

                if(point.containsKey("arrival_port_calc"))
                    strs.add(point.get("arrival_port_calc"));
            }
            ret.add(String.join(",", strs));

            try {
                if(point.getStringValueFor("timestamp").equals(arrivalTimestamp))
                    labelTuples=false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

//    public double getParkingRate(){
//        //long parkingTuples = getPoints().stream().filter(tuple -> Double.parseDouble(tuple.get("speed")) <2).count();
//        int parkingTuplesCount = getParkings().values().stream().mapToInt(item->item.getPoints().size()).sum();
//        double rate = (parkingTuplesCount*1.0)/ getPoints().size();
//        rate = Math.floor(rate * 1000.0) / 1000;
//        return rate;
//    }

//    public Map<List<DataPoint>, String> getPortChanges(Map<String, double[]> ports){
//        Map<List<DataPoint>, String> ret = new LinkedHashMap<>();
//        String prevBelongings = null;
//        List<DataPoint> packToSplit = new ArrayList<>();
//
//
//        for(DataPoint dataPoint : getPoints()){
//            try {
//                Map<String, Double> belonging = Ports.belongsToPort(ports, dataPoint);
//                //String joinedBelongings = String.join(", ", belonging.keySet());
//                String joinedBelongings = "";
//                //if(belonging.size()>0){
//                joinedBelongings = (belonging.size()>0 ? belonging.keySet().iterator().next(): "");
//                if(prevBelongings!=null && !joinedBelongings.equals(prevBelongings)){
//                    ret.put(packToSplit, prevBelongings);
//                    packToSplit = new ArrayList<>();
//
//                }
//                packToSplit.add(dataPoint);
//
//                //}
//                prevBelongings = joinedBelongings;
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        if(packToSplit.size()>0)
//            ret.put(packToSplit, prevBelongings);
//
//        if(getId().startsWith("0xa2160_12-05-15")){
//            String test="123";
//        }
//        return ret;
//    }
//
    public Map<String, Parking> getParkings(){
        Map<String, Parking> ret = new LinkedHashMap<>();
        List<DataPoint> window = new ArrayList<>();
        List<DataPoint> bufferToPrepend = new ArrayList<>();
        List<DataPoint> bufferToRemove = new ArrayList<>();
        List<DataPoint> prevTuplesBuffer = new ArrayList<>();
        List<DataPoint> parkingTuples = new ArrayList<>();
        double prevRate = 0;
        for(DataPoint point : getPoints()) {

            window.add(point);
            double rate =  window.stream().mapToDouble(tpoint -> Double.parseDouble(tpoint.get("speed"))).sum() / window.size();
            double speed = Double.parseDouble(point.get("speed"));


//            if(speed>2) {
//                bufferToRemove.add(point);
//                //bufferToPrepend.clear();
//            }else {
//                //bufferToRemove.clear();
//                bufferToPrepend.add(point);
//            }
            if(speed==0){
                String test="123";
            }
            if(/*speed<2 || */rate<=0.2) {
                if(parkingTuples.size()==0)
                    if (rate < prevRate)
                        bufferToPrepend = new ArrayList<>(prevTuplesBuffer);
                        //bufferToPrepend = new ArrayList<>(prevTuplesBuffer.subList(Math.min(4, prevTuplesBuffer.size())-1, prevTuplesBuffer.size()));

                parkingTuples.add(point);

            }else {
                if (rate > prevRate && (bufferToPrepend.size() + parkingTuples.size() - 1) > 0){ //
                    //}else if(rate>prevRate && parkingTuples.size()>1){ //
                    List<DataPoint> tuplesToAdd = new ArrayList();
                    tuplesToAdd.addAll(bufferToPrepend);
                    tuplesToAdd.remove(point);
                    tuplesToAdd.addAll(parkingTuples);

                    Parking parking = new Parking(tuplesToAdd);
                    ret.put(parking.getId(), parking);

                    parkingTuples.clear();
                    bufferToPrepend.clear();
                    bufferToRemove.clear();
                    //rate = 99999;
                }
            }
            prevRate = rate;
            prevTuplesBuffer.add(point);
            if(prevTuplesBuffer.size()>8)
                prevTuplesBuffer.remove(0);
            if(window.size()>8)
                window.remove(0);
        }

//        for(String name: ret.keySet()){
//            System.out.println(name);
//            System.out.println(String.join("\n", ret.get(name).stream().map(tuple-> tuple.get("speed")).collect(Collectors.toList())));
//
//        }
        return ret;
    }



    public void dispose(){
        for (DataPoint tuple : getPoints())
            tuple = null;
        points.clear();
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getSequence() {
        return sequence;
    }
}
