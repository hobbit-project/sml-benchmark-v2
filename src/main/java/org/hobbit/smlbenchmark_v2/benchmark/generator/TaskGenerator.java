package org.hobbit.smlbenchmark_v2.benchmark.generator;

import com.google.common.io.Resources;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.commons.io.IOUtils;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractTaskGenerator;
import org.hobbit.core.rabbit.*;
import org.hobbit.smlbenchmark_v2.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.hobbit.smlbenchmark_v2.Constants.*;


/**
 * @author Pavel Smirnov
 */

public class TaskGenerator extends AbstractTaskGenerator {
    private Logger logger;

    private final Boolean sequental = false;
    //private final Map<String, Integer> pointIndexes = new ConcurrentHashMap<>();
    //private final Map<String, Integer> tripIndexes = new ConcurrentHashMap<>();

    private final Map<String, Integer> pointIndexes = Collections.synchronizedMap(new HashMap<String, Integer>());
    private final Map<String, Integer> tripIndexes = Collections.synchronizedMap(new HashMap<String, Integer>());


    private final Map<String, Integer> shipTuplesCount = new HashMap<>();
    protected DataReceiver evalStorageNoficationsReceiver;

    private Semaphore genStartMutex;
    private Semaphore genFinishedMutex;
    private Semaphore systemFinishedMutex;

    //private final Map<String, String> tupleTimestamps = new HashMap<>();
    //private final Map<String, String> sentTasksMap = new HashMap<>();
    //private final Map<String, String> sentTasksMap = new ConcurrentHashMap<>();

    private Map<String, Map<String, Trip>> tripsPerShips;
    //private final Map<String, Map<String, KeyValue>> labelsPerShips = new HashMap<>();

    private final Map<String, String> sentTasksMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, List<String>> shipTasks = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, String> taskExpectations = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Long> taskSentTimestamps = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Integer> resentCounters = Collections.synchronizedMap(new HashMap<>());

    private List<String> shipsToSend = Collections.synchronizedList(new ArrayList<>());

    int queryType = -1;
    int dataFormat = 0;

    public List<String> logs = new ArrayList<>();
    public List<DataPoint> allPoints;
    public List<String> tuplesToSend = new ArrayList<>();

    long tuplesToSentByBenchmark;
    long tuplesToSendByGenerator;
    Channel evalStorageTaskGenChannel;
    QueueingConsumer exchangeQueueConsumer;

    int notificationsReceived=0;
    int notificationsReceivedTotal=0;
    //int recordsSent = 0;
    int tuplesLimit = 0;
    long lastReportedValue = 0;
    //long expectationsToStorageTime =0;
    long valDiff = 0;
    long expectationsSent = 0;
    long errors = 0;
    //String[] shipIdsToSend;

    String encryptionKey="encryptionKey";

    String formatTemplate;
    Timer timer;
    int timerPeriodSeconds = 5;
    protected DataSender sender2EvalStorage;

    Boolean gerenationStarted = false;
    private String customQueueName;
    private String datasetFilePath = DATASET_FILE_NAME;
    private SimpleFileSender simpleFileSender;



    @Override
    public void init() throws Exception {
        // Always init the super class first!
        super.init();
        logger = LoggerFactory.getLogger(TaskGenerator.class.getName()+"_"+getGeneratorId());
        logger.debug("Init()");

        dataGenReceiver = null;

        if(System.getenv().containsKey(ENCRYPTION_KEY_NAME))
            encryptionKey = System.getenv().get(ENCRYPTION_KEY_NAME);

        if(System.getenv().containsKey("TUPLES_LIMIT")) {
            logger.debug("TUPLES_LIMIT={}",System.getenv().get("TUPLES_LIMIT"));
            tuplesLimit = Integer.parseInt(System.getenv().get("TUPLES_LIMIT"));
        }

        if(System.getenv().containsKey("QUERY_TYPE"))
            queryType = Integer.parseInt(System.getenv().get("QUERY_TYPE"));

        if(System.getenv().containsKey("DATA_FORMAT"))
            dataFormat = Integer.parseInt(System.getenv().get("DATA_FORMAT"));

        if(System.getenv().containsKey(Constants.DATA_QUEUE_NAME_KEY))
            customQueueName = System.getenv().get(Constants.DATA_QUEUE_NAME_KEY);

        if(System.getenv().containsKey(DATASET_FILE_NAME_KEY))
            datasetFilePath = System.getenv().get(DATASET_FILE_NAME_KEY);

        if(dataFormat==1){
            URL url = Resources.getResource("dataPointTemplate.rdf");
            formatTemplate = Resources.toString(url, CHARSET);
        }

        if(queryType<=0){
            Exception ex = new Exception("Query type is not specified correctly");
            logger.error(ex.getMessage());
            throw ex;
        }

        int dataGeneratorId = getGeneratorId();
        int numberOfGenerators = getNumberOfGenerators();

        genFinishedMutex = new Semaphore(0);
        genStartMutex = new Semaphore(0);
        systemFinishedMutex = new Semaphore(0);

        String exchangeQueueName = this.generateSessionQueueName(ACKNOWLEDGE_QUEUE_NAME);

        logger.debug("Init (genId={}, queryType={}, tuplesLimit={}", dataGeneratorId, queryType, tuplesLimit);

//        sender2EvalStorage = DataSenderImpl.builder().queue(
//                this.getFactoryForOutgoingDataQueues(),
//                this.generateSessionQueueName(ACKNOWLEDGE_STATUS_QUEUE_NAME)).build();
        if(customQueueName!=null){
            simpleFileSender = SimpleFileSender.create(this.outgoingDataQueuefactory, customQueueName);

        }else{
            evalStorageNoficationsReceiver = ExchangeDataReceiver.builder().dataHandler(new DataHandler() {
                public void handleData(byte[] body) {
                    if (body.length > 0) {

                        String encryptedTaskId = new String(body);
                        logger.trace("Received acknowledgement: {}", encryptedTaskId);
                        increaseTotalCounter();
                        if (sentTasksMap.containsKey(encryptedTaskId)) {
                            notificationsReceived++;
                            String shipId = sentTasksMap.get(encryptedTaskId);

                            if (shipsToSend.contains(shipId)) {
                                updateIndexes(shipId);

                                try {
                                    if (shipsToSend.contains(shipId))
                                        sendData(shipId);
                                } catch (Exception e) {
                                    logger.error("Failed to send data: {}", e.getMessage());
                                }
                                //sentTasksMap.remove(encryptedTaskId);
                            }

                        }

                    }
                }
                //}).maxParallelProcessedMsgs(RECEIVER_THREADS).queue(this.getFactoryForIncomingDataQueues(), exchangeQueueName).build();
            }).maxParallelProcessedMsgs(RECEIVER_THREADS).queue(new ExchangeRabbitFactory(this.createConnection()), exchangeQueueName).build();
        }

        initData();
    }

    public synchronized void updateIndexes(String shipId){

        int tripIndex = (tripIndexes.containsKey(shipId)?tripIndexes.get(shipId):0);
        int pointIndex = (pointIndexes.containsKey(shipId)? pointIndexes.get(shipId):0);

        Trip trip = ((Trip) (tripsPerShips.get(shipId).values().toArray()[tripIndex]));
        List<DataPoint> tripPoints = trip.getPoints();

        pointIndex++;
        //if all point of the trip have been sent off switch to the next trip
        if (pointIndex >= tripPoints.size()) {
            tripIndex++;
            pointIndex = 0;
        }

        if (tripIndex >= tripsPerShips.get(shipId).size()) {
            shipsToSend.remove(shipId);
        }

        tripIndexes.put(shipId, tripIndex);
        pointIndexes.put(shipId, pointIndex);

    }

    private synchronized void increaseTotalCounter(){
        notificationsReceivedTotal++;
    }

    private void initData() throws Exception {
        //getPointsPerShip(Paths.get("data","vessel24hpublic_fixed.csv"),0);
        //getPointsPerShip(Paths.get("data","debs2018_training_fixed_2.csv"), tuplesLimit);

        //String[] lines = Utils.readFile(Paths.get("data","1000rowspublic_fixed.csv"), tuplesLimit);
        //String[] lines = Utils.readFile(Paths.get("data","debs2018_training_fixed_4.csv"), tuplesLimit);
        Dataset dataset = new Dataset(this.logger);
        //String[] lines = Utils.readFile(Paths.get("data","debs2018_training_labeled.csv"), tuplesLimit);

        String[] lines = dataset.readFile(Paths.get(datasetFilePath), (customQueueName!=null?tuplesLimit:0));
        tripsPerShips = dataset.getTripsPerShips(lines, (customQueueName!=null?0:tuplesLimit));

        //String test="123";
        tuplesToSentByBenchmark = tripsPerShips.values().stream().mapToInt(map->map.values().stream().mapToInt(trip->trip.getPoints().size()).sum()).sum();

        int generatorId = getGeneratorId();
        int numberOfGenerators = getNumberOfGenerators();
        if(numberOfGenerators>1) {
            Map<Integer, List<String>> balancedShipIds = balanceShipsAmongGenerators(tripsPerShips, numberOfGenerators);
            List<String> shipIdsToServe = balancedShipIds.get(generatorId);
            Map<String, Map<String, Trip>> shipsToServe = new HashMap<>();
            for(String shipId : shipIdsToServe)
                shipsToServe.put(shipId, tripsPerShips.get(shipId));
            tripsPerShips = shipsToServe;
        }

//        String testId = "0x023db8bbb80aa3144444cefe369f4e4edfedd70f";
        //String testId = "0x4b390996aa0542a24694581fac78e80cb3e5a280";
//        List<List<DataPoint>> testShipTrips = tripsPerShips.get(testId);
//        tripsPerShips.clear();
//        tripsPerShips.put(testId, testShipTrips);

        for(String shipId : tripsPerShips.keySet()){
        //for(String shipId : new String[]{ tripsPerShips.keySet().iterator().next() }){
            shipsToSend.add(shipId);
            List<DataPoint> shipPoints = tripsPerShips.get(shipId).values().stream().flatMap(l-> l.getPoints().stream()).collect(Collectors.toList());
            //pointsPerShip.put(shipId, shipPoints);
            tuplesToSendByGenerator +=shipPoints.size();
            shipTuplesCount.put(shipId, shipPoints.size());
            shipTasks.put(shipId, new ArrayList<>());
            if(sequental)
                allPoints.addAll(shipPoints);
        }
//        if(tuplesLimit>0)
//            tuplesToSendByGenerator = Math.min(tuplesLimit, tuplesToSendByGenerator);
     String test="123";

    }

    public Map<Integer, List<String>> balanceShipsAmongGenerators(Map<String, Map<String, Trip>> tripsPerShips, int numberOfGenerators){
        logger.debug("Balancing ships among muptiple generators");
        Map<Integer, List<String>> shipsPerTuplesCount = new HashMap<>();
        Map<String, Integer> tuplesCountPerShips = new HashMap<>();
        for(String shipId : tripsPerShips.keySet()){
            int tuplesCountPerShip = 0;
            Map<String, Trip> thisShipTrips = tripsPerShips.get(shipId);
            for(Trip trip : thisShipTrips.values())
                tuplesCountPerShip+=trip.getPoints().size();
            tuplesCountPerShips.put(shipId, tuplesCountPerShip);

            List<String> shipIdsPerCount = (shipsPerTuplesCount.containsKey(tuplesCountPerShip)? shipsPerTuplesCount.get(tuplesCountPerShip): new ArrayList<String>());
            shipIdsPerCount.add(shipId);
            shipsPerTuplesCount.put(tuplesCountPerShip, shipIdsPerCount);
        }

        List<Integer> keySet = new ArrayList(shipsPerTuplesCount.keySet());
        Collections.sort(keySet, Collections.reverseOrder());

        Map<Integer, List<String>> shipsPerGens = new HashMap();
        Map<Integer, Integer> tuplesPerGens = new HashMap();
        for(int i=0; i<numberOfGenerators;i++)
            tuplesPerGens.put(i,0);

        for(int countKey : keySet)
            for(String shipId : shipsPerTuplesCount.get(countKey)){
                int genId = getThelessLoadedGen(tuplesPerGens);
                List<String> shipsPerGen = (shipsPerGens.containsKey(genId)? shipsPerGens.get(genId) : new ArrayList<>());
                shipsPerGen.add(shipId);
                shipsPerGens.put(genId, shipsPerGen);

                int tuplesPerGen = (tuplesPerGens.containsKey(genId)? tuplesPerGens.get(genId) : 0);
                tuplesPerGen+=tuplesCountPerShips.get(shipId);
                tuplesPerGens.put(genId, tuplesPerGen);
            }
        logger.debug("Balancing ships finished: {}", tuplesPerGens.toString());
        return shipsPerGens;
    }

    private static int getThelessLoadedGen(Map<Integer, Integer> tuplesPerGens){

        int lessLoweredGenId = 1;
        int lessLoweredGenTuplesCount = 999999999;
        for(Integer genId : tuplesPerGens.keySet()){
            if(tuplesPerGens.get(genId)<lessLoweredGenTuplesCount){
                lessLoweredGenId=genId;
                lessLoweredGenTuplesCount = tuplesPerGens.get(genId);
            }
        }

        return lessLoweredGenId;
    }


    @Override
    protected void generateTask(byte[] bytes) throws Exception {

    }

    @Override
    public void run() throws Exception {
        sendToCmdQueue(Commands.TASK_GENERATOR_READY_SIGNAL);
        genStartMutex.acquire();

        try {
            generateData();
        } catch (Exception e) {
            logger.error("Error while generating data");
            throw e;
        }

        sendToCmdQueue(Commands.TASK_GENERATION_FINISHED);

        if(customQueueName!=null)
            sendDataToSimpleFileSender();
        else{
            this.systemFinishedMutex.acquire();
            sendExpectationsToStorage();
        }
        //logger.debug("Sending finished commands");

//        this.dataGenReceiver.closeWhenFinished();
//        this.sender2System.closeWhenFinished();
//        this.sender2EvalStore.closeWhenFinished();
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == Commands.TASK_GENERATOR_START_SIGNAL){
            logger.debug("TASK_GENERATOR_START_SIGNAL received");
            genStartMutex.release();
        }
        if (command == SYSTEM_FINISHED_SIGNAL)
            systemFinishedMutex.release();
        super.receiveCommand(command, data);
    }



    public void generateData() throws Exception {
        logger.debug("generateData()");

        long started = new Date().getTime();

        startTimer();

        logger.debug("Start sending {} tuples for {} ships", tuplesToSendByGenerator, shipsToSend.size());

        sendData(null);
        if(customQueueName!=null) {
            Boolean stop = false;
            while (!stop)
                stop = sendData(null);
        }else
            genFinishedMutex.acquire();

        timer.cancel();

        double took = (new Date().getTime() - started)/1000.0;
        logger.debug("Finished after {} tuples sent. Took {} s. Avg: {} tuples/s", taskSentTimestamps.size(), took, Math.round(taskSentTimestamps.size()/took));

    }


//    private void waitNotifications(Callback<byte[],Boolean> execute) throws Exception {
//        QueueingConsumer.Delivery delivery = exchangeQueueConsumer.nextDelivery();
//        execute.call(delivery.getBody());
//    }

    private void startTimer(){
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                valDiff = (taskSentTimestamps.size() - lastReportedValue)/timerPeriodSeconds;
                logger.debug("{} tuples sent. Unfinished ships:{}. Curr: {} tuples/s; Notifs: {} Notifs total: {}", taskSentTimestamps.size(), shipsToSend.size(), valDiff, notificationsReceived, notificationsReceivedTotal);
                lastReportedValue = taskSentTimestamps.size();

                String [] shipIdsToBeResent = shipsToSend.toArray(new String[0]);
                for(String shipId: shipIdsToBeResent){
                    List<String> thisShipTasks = shipTasks.get(shipId);
                    if(thisShipTasks.size()>0) {
                        String lastTaskId = thisShipTasks.get(thisShipTasks.size()-1);
                        if(taskSentTimestamps.containsKey(lastTaskId)){
                            if (new Date().getTime() - taskSentTimestamps.get(lastTaskId) > 30000) {
                                int resentCounter = (resentCounters.containsKey(lastTaskId)? resentCounters.get(lastTaskId):0);
                                //if (resentCounter<2){
                                    try {

                                        //                                int pointIndex = pointIndexes.get(shipId);
                                        //                                int tripIndex = tripIndexes.get(shipId);
                                        //                                pointIndex--;
                                        //                                if(pointIndex<0){
                                        //                                    tripIndex--;
                                        //                    taskSentTimestamps                pointIndex=0;
                                        //                                }
                                        //                                if(tripIndex<0)
                                        //                                    tripIndex=0;
                                        //                                tripIndexes.put(shipId, tripIndex);
                                        //                                pointIndexes.put(shipId, pointIndex);
                                        logger.debug("Resending (#{}) last task for {}", resentCounter, shipId);
                                        sendData(shipId);
                                        resentCounter++;

                                        resentCounters.put(lastTaskId, resentCounter);

                                        //if(resentCounter==4)
                                         //   logger.debug("Resending limit({}) is reached for {}", resentCounter, shipId);

                                    } catch (Exception e) {
                                        logger.error("Failed to resend task for " + shipId);
                                    }
//                                }else{
//
//                                }
                            }
                        }else
                            logger.error("No timestamp for {}, shipId {}"+lastTaskId, shipId);
//                        try {
//                            sender2EvalStorage.sendData(thisShipTasks.get(thisShipTasks.size() - 1).getBytes());
//                        } catch (IOException e) {
//                            logger.error("Failed to send status request");
//                        }
                    }
                }

                if (notificationsReceived>= tuplesToSendByGenerator && notificationsReceivedTotal>=tuplesToSentByBenchmark)
                    genFinishedMutex.release();

//                for(String log: logs.toArray(new String[0]))
//                    logger.debug(log);
//                logs.clear();
//                System.exit(0);
            }
        }, 1000, timerPeriodSeconds*1000);

    }

    private synchronized Boolean sendData(String shipId) throws Exception {
        Boolean stop = false;

        if (Math.max(taskSentTimestamps.size(), notificationsReceivedTotal)>=tuplesToSentByBenchmark)
            return true;

        if(sequental)
            sendSequental();
        else
            sendParallel(shipId);

        if (shipsToSend.size()==0)
            stop=true;

        return stop;
    }

    private void sendSequental() throws Exception {
//        DataPoint dataPoint = allPoints.get(recordsSent);
//        sendPoint(dataPoint, recordsSent);
    }

    private synchronized void sendParallel(String singleShipId){
        Map <String, String> uniqueTaskIds = new HashMap<>();

        String[] shipIdsToSend = (singleShipId!=null && shipsToSend.contains(singleShipId) ? new String[]{ singleShipId }: shipsToSend.toArray(new String[0]));

        for(String shipId : shipIdsToSend){

            try {

                int tripIndex = (tripIndexes.containsKey(shipId)?tripIndexes.get(shipId):0);
                int pointIndex = (pointIndexes.containsKey(shipId)? pointIndexes.get(shipId):0);


                String encryptedTaskId = null;
                while (encryptedTaskId == null && tripIndex < tripsPerShips.get(shipId).size()) {
                    Trip trip = ((Trip) (tripsPerShips.get(shipId).values().toArray()[tripIndex]));
                    List<DataPoint> tripPoints = trip.getPoints();
                    DataPoint dataPoint = tripPoints.get(pointIndex);

                    try {
                        //String taskId = "gen_"+String.valueOf(getGeneratorId())+"_"+shipId.substring(0,5)+"_"+tripIndex+"_task_"+String.valueOf(pointIndex);
                        String taskId = "gen_" + String.valueOf(getGeneratorId()) + "_" + shipId + "_" + tripIndex + "_task_" + String.valueOf(pointIndex);
                        //String taskId = "gen_"+String.valueOf(getGeneratorId())+"_"+shipId.substring(0,5)+"_"+tripIndex+"_task_"+String.valueOf(pointIndex)+"_"+new Date().getTime()+"_"+Math.random()*Math.random();
                        logs.add("sendPoint(" + shipId + "): " + tripIndex + ", " + pointIndex + " = " + taskId);
                        encryptedTaskId = sendPoint(dataPoint, pointIndex, taskId);
                        if(customQueueName!=null)
                            updateIndexes(shipId);
                        if (encryptedTaskId == null)
                            pointIndex = 99999999; //switch to the next trip of the ship
                        //uniqueTaskIds.put(taskId,"");
                    } catch (Exception e) {
                        logger.error("Problem with sendPoint(): " + e.getMessage());
                    }


                    //pointIndex++;
                    //if all point of the trip have been sent off switch to the next trip
//                    if (pointIndex >= tripPoints.size()) {
//                        tripIndex++;
//                        pointIndex = 0;
//                    }
//
//                    if (tripIndex >= tripsPerShips.get(shipId).size())
//                        shipsToSend.remove(shipId);
//                    else {
//                        tripIndexes.put(shipId, tripIndex);
//                        pointIndexes.put(shipId, pointIndex);
//                    }

                }

//                    tripIndexes.put(shipId, tripIndex);
//                    pointIndexes.put(shipId, pointIndex);

            }
            catch (Exception e){
                logger.error("Problem with sendParallel(): "+e.getMessage());
            }
        }
        String test="123";
    }

    private String sendPoint(DataPoint dataPoint, int orderingIndex, String taskId) throws Exception {

        String shipId = dataPoint.getValue("ship_id").toString();
        String tripId = dataPoint.getValue("trip_id").toString();

        //String taskId = "gen_"+String.valueOf(getGeneratorId())+"_task_"+String.valueOf(orderingIndex);

        String raw = dataPoint.getStringValueFor("raw");

        String sendToSystem = (dataFormat==1?prepareRdfFormat(dataPoint, String.valueOf(new Date().getTime())+Math.random()): raw);

        logger.trace("sendTaskToSystemAdapter({})->{}", taskId, sendToSystem);


        long taskSentTimestamp = System.currentTimeMillis();
        if(customQueueName!=null)
            tuplesToSend.add(sendToSystem);
        else
            sendTaskToSystemAdapter(taskId, sendToSystem.getBytes());

        String encryptedTaskId = encryptTaskId(taskId, encryptionKey);

        String tupleTimestamp = dataPoint.get("timestamp");
        String expectedPortName = (dataPoint.containsKey("arrival_port_calc")? dataPoint.get("arrival_port_calc"):"");
        String expectedArrivalTime = (dataPoint.containsKey("arrival_calc")? dataPoint.get("arrival_calc"):"");

        String expectation = "";

        if(queryType==1)
            expectation = dataPoint.get("trip_id") + "," + orderingIndex + "," + tupleTimestamp + ","+ expectedPortName;
        else if(!expectedPortName.equals("") && !expectedArrivalTime.equals(""))
            expectation = expectedPortName+ "," + expectedArrivalTime;

        put(encryptedTaskId, shipId, taskSentTimestamp, expectation);
        //sendExpectation(encryptedTaskId, taskSentTimestamp, expectation);

        return encryptedTaskId;
    }

    public synchronized void put(String encryptedTaskId, String shipId, long taskSentTimestamp, String expectation){
        List<String> thisShipTasks = shipTasks.get(shipId);
        thisShipTasks.add(encryptedTaskId);
        shipTasks.put(shipId, thisShipTasks);
        sentTasksMap.put(encryptedTaskId, shipId);
        taskSentTimestamps.put(encryptedTaskId, taskSentTimestamp);
        if(!expectation.equals(""))
            taskExpectations.put(encryptedTaskId, expectation);
    }

    public static String encryptTaskId(String string, String encryptionKey){
        String ret = String.valueOf(hash+656).substring(2)+String.valueOf((hash+4)*(hash+2));

        return ret;
    }


    public void sendExpectationsToStorage(){
        logger.debug("sendExpectationsToStorage()");
        long started = new Date().getTime();
        for(String shipId : shipTasks.keySet())
            for(String encryptedTaskId : shipTasks.get(shipId))
                if(taskExpectations.containsKey(encryptedTaskId)){
                    try {
                        sendExpectation(encryptedTaskId, taskSentTimestamps.get(encryptedTaskId), taskExpectations.get(encryptedTaskId));
                        expectationsSent++;
                    } catch (IOException e) {
                        logger.error("Failed to send expectation: {}",e.getMessage());
                        errors++;
                    }

                }

        long took = (new Date().getTime() - started);
        logger.debug("sendExpectationsToStorage({}) took {} s", expectationsSent, (took/1000.00));
    }

    private void sendExpectation(String taskId, long taskSentTimestamp, String sendToStorage) throws IOException {
        logger.trace("sendTaskToEvalStorage({})=>{}", taskId, sendToStorage.getBytes());
        sendTaskToEvalStorage(taskId, taskSentTimestamp, sendToStorage.getBytes());
    }

    private String prepareRdfFormat(DataPoint dataPoint, String taskId){
        String[] splitted = dataPoint.get("raw").split(",");

        String ret = formatTemplate.toString();
        ret = ret.replaceAll("\\{position_uri}",BENCHMARK_URI+"/position#"+taskId);
        ret = ret.replaceAll("\\{position_course}", splitted[5]);
        ret = ret.replaceAll("\\{position_heading}",splitted[6]);

        ret = ret.replaceAll("\\{point_uri}", BENCHMARK_URI+"/point#"+taskId);
        ret = ret.replaceAll("\\{point_lat}",splitted[4]);
        ret = ret.replaceAll("\\{point_lon}",splitted[3]);

        ret = ret.replaceAll("\\{time_uri}", BENCHMARK_URI+"/time#"+taskId);
        ret = ret.replaceAll("\\{time_value}", splitted[7]);

        ret = ret.replaceAll("\\{trajectory_uri}", BENCHMARK_URI+"/trajectory#"+dataPoint.get("trip_id"));
        ret = ret.replaceAll("\\{trajectory_departure}", splitted[8]);

        ret = ret.replaceAll("\\{ship_uri}", BENCHMARK_URI+"/ship#"+splitted[0]);
        ret = ret.replaceAll("\\{ship_type}", splitted[1]);
        ret = ret.replaceAll("\\{ship_speed}", splitted[2]);
        ret = ret.replaceAll("\\{ship_draught}", (splitted.length>9?splitted[9]:"none"));

        return ret;
    }


    public void sendDataToSimpleFileSender() throws IOException{
        logger.debug("Sending data via a simple file sender");
        String str = String.join("\n", tuplesToSend.toArray(new String[0]));
        byte[] bytesToSend = str.getBytes();
        simpleFileSender.streamData(new ByteArrayInputStream(bytesToSend), customQueueName+".rdf");
    }

    public void close() throws IOException {
        logger.debug("close()");
        //evalStorageNoficationsReceiver.closeWhenFinished();
        //super.close();
        if (dataGenReceiver!=null)
            IOUtils.closeQuietly(this.dataGenReceiver);
        if (sender2EvalStorage!=null)
            IOUtils.closeQuietly(this.sender2EvalStore);
        if (sender2System!=null)
            IOUtils.closeQuietly(this.sender2System);

        if(simpleFileSender!=null)
            IOUtils.closeQuietly(simpleFileSender);

        if(evalStorageNoficationsReceiver!=null)
            IOUtils.closeQuietly(evalStorageNoficationsReceiver);

        logger.debug("closed");
        //throw new IOException("Forcing container exit");

    }
}
