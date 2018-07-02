package org.hobbit.smlbenchmark_v2.utils;

import org.hobbit.smlbenchmark_v2.benchmark.generator.DataPoint;

import java.text.ParseException;
import java.util.List;

import static org.hobbit.smlbenchmark_v2.Constants.DEFAULT_DATE_FORMAT;

public class Parking {
    List<DataPoint> dataPoints;

    public Parking(List<DataPoint> dataPoints){
        this.dataPoints = dataPoints;
    }

    public String getId(){
        String ret = getPoints().get(0).get("timestamp")+" - "+ getPoints().get(getPoints().size()-1).get("timestamp");
        return ret;
    }

    public List<DataPoint> getPoints() {
        return dataPoints;
    }

    public long getDuration(){

        try {
            long started = DEFAULT_DATE_FORMAT.parse(getPoints().get(0).get("timestamp")).getTime();
            long finished = DEFAULT_DATE_FORMAT.parse(getPoints().get(getPoints().size()-1).get("timestamp")).getTime();
            return finished-started;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return -1;

    }
}
