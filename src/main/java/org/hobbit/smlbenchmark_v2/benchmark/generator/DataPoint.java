package org.hobbit.smlbenchmark_v2.benchmark.generator;


import org.hobbit.sdk.KeyValue;

import javax.swing.plaf.SeparatorUI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataPoint extends KeyValue {

    private String separator;
    private List<String> headings;
    private String[] splitted;

    public DataPoint(String string, List<String> headings, String separator){
        this(string, headings, separator, new String[]{"trip_id", "arrival_calc", "arrival_port_calc", "label"});
    }

    public DataPoint(String string, List<String> headings, String separator, String[] valsToExcludeFromRaw){
        super();

        this.headings = headings;
        this.separator = separator;


        //List<String> splitted = new ArrayList<>(Arrays.asList(string.split(separator)));
        this.splitted = string.split(separator);
        setValue("ship_id", splitted[0]);
        setValue("speed", splitted[headings.indexOf("speed")]);
        setValue("departure_port_name", splitted[headings.indexOf("departure_port_name")]);
        setValue("timestamp", splitted[headings.indexOf("timestamp")]);

        for(String valToTake : valsToExcludeFromRaw){
            String value = null;
            if (headings.contains(valToTake)) {
                int index = headings.indexOf(valToTake);
                if(splitted.length>index)
                    value = splitted[index];
            }
            if(value!=null)
                setValue(valToTake, value);
        }

        setValue("raw", toStringExcept(Arrays.asList(valsToExcludeFromRaw)));

    }


    public String get(String propertyName){
        try {
            if(getValue(propertyName)!=null)
                return getValue(propertyName).toString();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String toStringExcept(List<String> toExcluDe){
        List<String> toJoin = new ArrayList<>();

        for(int i=0; i<headings.size(); i++) {
            if (toExcluDe == null || !toExcluDe.contains(headings.get(i)))
                if (i < splitted.length)
                    toJoin.add(splitted[i]);
                else
                    toJoin.add("");
        }
        for(Object rest: super.getMap().keySet())
            if (!rest.toString().equals("raw") && !headings.contains(rest.toString()))
                toJoin.add(get(rest.toString()));
        String ret = String.join(separator, toJoin);
        return ret;
    }


    public String toString(){
        return toStringExcept(null);
    }



}
