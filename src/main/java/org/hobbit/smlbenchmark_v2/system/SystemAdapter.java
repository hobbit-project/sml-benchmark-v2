package org.hobbit.smlbenchmark_v2.system;

import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.sdk.JenaKeyValue;
import org.hobbit.smlbenchmark_v2.benchmark.generator.DataPoint;
import org.hobbit.smlbenchmark_v2.utils.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.hobbit.smlbenchmark_v2.Constants.*;

/**
 * @author Pavel Smirnov
 */

public class SystemAdapter extends AbstractSystemAdapter {
    private static final String HOBBIT_SYSTEM_CONTAINER_ID_KEY = "";
    private static JenaKeyValue parameters;
    private Logger logger = LoggerFactory.getLogger(SystemAdapter.class);
    private int queryType = -1;
    private final Map<String, Integer> tuplesPerShip = new HashMap<String, Integer>();
    private final Map<String, Integer> tempTuplesPerShip = new HashMap<String, Integer>();
    private Map<String, Map<String, Trip>> shipTrips;
    private Map<String, Map<String, DataPoint>> shipPoints = new HashMap<>();

    Timer timer;
    boolean timerStarted=false;
    long lastReportedValue = 0;
    int tuplesToMiss=0;
    //long tuplesReceived=0;
    //long resultsSent=0;
    //private final Map<String, String> tuplesReceived = Collections.synchronizedMap(new HashMap<>());
    //private final Map<String, String> resultsSent = Collections.synchronizedMap(new HashMap<>());

    private final List<String> tuplesReceived = Collections.synchronizedList(new ArrayList<>());
    private final List<String> resultsSent = Collections.synchronizedList(new ArrayList<>());

    long errors=0;
    int timerPeriodSeconds = 5;
    int systemContainerId = 0;
    int systemInstancesCount = 1;

//    public SystemAdapter(){
//        super(RECEIVER_THREADS);
//
//    }

    @Override
    public void init() throws Exception {
        logger.debug("Init()");
        super.init();

        timer = new Timer();

        // Your initialization code comes here...
        parameters = new JenaKeyValue.Builder().buildFrom(systemParamModel);

        if(!parameters.containsKey(BENCHMARK_URI+"#slaveNode")) {
            JenaKeyValue slaveParameters = new JenaKeyValue(parameters);
            slaveParameters.put(BENCHMARK_URI+"#slaveNode", "TRUE");
            createContainer(SYSTEM_IMAGE_NAME, new String[]{ Constants.SYSTEM_PARAMETERS_MODEL_KEY+"="+ slaveParameters.encodeToString() });
        }else
            logger = LoggerFactory.getLogger(SystemAdapter.class.getCanonicalName()+"_slave");


        queryType = parameters.getIntValueFor(QUERY_TYPE_KEY);
        if(queryType<=0){
            Exception ex = new Exception("Query type is not specified correctly");
            logger.error(ex.getMessage());
            throw ex;
        }

        logger.debug("Init finished. SystemModel: "+parameters.encodeToString()+" sender: "+(this.sender2EvalStore!=null?"not null": "null") );
        startTimer();

//        Dataset utils = new Dataset(this.logger);
//        String[] lines = utils.readFile(Paths.get(DATASET_FILE_NAME), 0);
//
//        shipTrips = utils.getTripsPerShips(lines);
//        for(String shipId : shipTrips.keySet()){
//            //for(String shipId : new String[]{ shipTrips.keySet().iterator().next() }){
//            List<DataPoint> shipPointsList = shipTrips.get(shipId).values().stream().flatMap(l-> l.getPoints().stream()).collect(Collectors.toList());
//            Map<String, DataPoint> thisShipPoints = new HashMap<>();
//            for(DataPoint point : shipPointsList){
//                thisShipPoints.put(point.get("timestamp"), point);
//            }
//            shipPoints.put(shipId, thisShipPoints);
//        }

    }

    private void startTimer(){
        if(timerStarted)
            return;
        timerStarted = true;
        timer.schedule(new TimerTask() {
            @Override
            public void run() {

                long valDiff = (tuplesReceived.size() - lastReportedValue)/timerPeriodSeconds;
                logger.debug("{} tuples received. Curr: {} tuples/s. ({} parallel ships); {} results sent {}", tuplesReceived.size(), valDiff, tempTuplesPerShip.size(), resultsSent.size(), (errors>0?errors+" errors":""));
                lastReportedValue = tuplesReceived.size();
                tempTuplesPerShip.clear();
            }
        }, 1000, timerPeriodSeconds*1000);

    }


    @Override
    public void receiveGeneratedData(byte[] data) {
        // handle the incoming data as described in the benchmark description
        logger.trace("receiveGeneratedData("+new String(data)+"): "+new String(data));
    }

    public synchronized void putReceived(String taskId){
        tuplesReceived.add(taskId);
    }

    public synchronized void putSent(String taskId){
        resultsSent.add(taskId);
    }

    @Override
    public void receiveGeneratedTask(String taskId, byte[] data) {
        //startTimer();
        // handle the incoming task and create a result
        String input = new String(data);
        logger.trace("receiveGeneratedTask({})->{}",taskId, input);

        putReceived(taskId);

        String[] splitted = input.split(",");
        String shipId = splitted[0];
        String timestamp = splitted[7];

        //logger.info(shipId+" "+timestamp);

        int tuplesOfTheShip =  (tuplesPerShip.containsKey(shipId)? tuplesPerShip.get(shipId): 0);

        tuplesOfTheShip++;

        String result = "null";
        try {
            // Send the result to the evaluation storage
            result = splitted[8];
            if(queryType==2)
                //result = result+","+splitted[7];
                result = splitted[7]+","+result;
                //result = splitted[7];

            if(tuplesOfTheShip>=30 &&  shipPoints.containsKey(shipId)){
                DataPoint dataPoint = shipPoints.get(shipId).get(timestamp);
                if(dataPoint.containsKey("arrival_port_calc"))
                    result = dataPoint.get("arrival_port_calc");
                if (queryType == 2 && dataPoint.containsKey("arrival_calc"))
                    result += "," + dataPoint.get("arrival_calc");
            }
        } catch (Exception e) {
            errors++;
            //logger.error("Processing error: {}", e.getMessage());
        }

        logger.trace("sendResultToEvalStorage({})->{}", taskId, result);
        try {
//            if(tuplesToMiss==0)
//                tuplesToMiss=1;
//            else
            sendResultToEvalStorage(taskId, result.getBytes());
            putSent(taskId);
        } catch (Exception e) {
            logger.error("sendResultToEvalStorage error: {}", e.getMessage());
        }
        tuplesPerShip.put(shipId, tuplesOfTheShip);
        tempTuplesPerShip.put(shipId, tuplesOfTheShip);

    }

    @Override
    public void close() throws IOException {
        timer.cancel();
      // Free the resources you requested here
        logger.debug("close()");
        try {
            super.close();
        }
        catch (Exception e){

        }
        //throw new IOException("Closed");
    }

}

