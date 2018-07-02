package org.hobbit.smlbenchmark_v2.benchmark;

import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractEvaluationStorage;
import org.hobbit.core.data.Result;
import org.hobbit.core.data.ResultPair;
import org.hobbit.core.rabbit.*;
import org.hobbit.sdk.SerializableResult;
import org.hobbit.sdk.evalStorage.ResultPairImpl;
import org.hobbit.smlbenchmark_v2.utils.ExchangeDataSender;
import org.hobbit.smlbenchmark_v2.utils.ExchangeRabbitFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static org.hobbit.core.Commands.DOCKER_CONTAINER_TERMINATED;
import static org.hobbit.core.Commands.EVAL_MODULE_FINISHED_SIGNAL;
import static org.hobbit.smlbenchmark_v2.Constants.*;
import static org.hobbit.smlbenchmark_v2.benchmark.generator.TaskGenerator.encryptTaskId;


/**
 * @author Pavel Smirnov
 */


public class EvalStorage extends AbstractEvaluationStorage {
    private static final Logger logger = LoggerFactory.getLogger(EvalStorage.class);
    protected Exception exception;

    private Map<String, ResultPair> results = Collections.synchronizedMap(new HashMap<String, ResultPair>());
    String encryptionKey="encryptionKey";

    public Object [] tasksLists;

    protected DataSender sender2TaskGen;
    DataReceiver taskGenStatusRequestsReceiver;

//    private final Map<String, String> actualResponses = new ConcurrentHashMap<String, String>();
//    private final Map<String, Long> responseReceivedTimestamps = new ConcurrentHashMap<String, Long>();
//
//    private final Map<String, String> expectedResponses = new HashMap<String, String>();
//    private final Map<String, Long> taskSentTimestamps = new HashMap<String, Long>();

    private final Map<String, String> actualResponses = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Long> responseReceivedTimestamps = Collections.synchronizedMap(new HashMap<>());

    private final Map<String, String> expectedResponses = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Long> taskSentTimestamps = Collections.synchronizedMap(new HashMap<>());

    private final Map<String, String> sentBuffer = Collections.synchronizedMap(new HashMap<>());
    //private final List<String> sentBuffer = new ArrayList<>();
    private final Map<String, String> notConfirmedBuffer = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, String> lastSentTasks = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Long> lastSentTasksTimestamps = Collections.synchronizedMap(new HashMap<>());

    //private final Map<String, List<String>> shipTasks = new HashMap<>();
    //private final Map<String, Map<Long, String>> tuplesPerTrips = new HashMap<>();
    private final Map<String, Map<Long, String>> tuplesPerTrips = Collections.synchronizedMap(new HashMap<>());

//    private String exchangeQueueName;
//    private Channel exchangeChannel;
    private String experimentUri;
    private int queryType=-1;

    long lastReportedValue = 0;
    long lastReportedValue2 = 0;
    Timer timer;
    int timerPeriodSeconds = 3;
    boolean timerStarted=false;
    List<String> notificationsQueue = new ArrayList<>();
    private int ignoredTasks=0;

    public EvalStorage(){
        super(RECEIVER_THREADS);
    }

    @Override
    public void init() throws Exception {
        super.init();
        logger.debug("Init()");

        if(System.getenv().containsKey(ENCRYPTION_KEY_NAME))
            encryptionKey = System.getenv().get(ENCRYPTION_KEY_NAME);

        if(System.getenv().containsKey("QUERY_TYPE"))
            queryType = Integer.parseInt(System.getenv().get("QUERY_TYPE"));


        if (System.getenv().containsKey("HOBBIT_EXPERIMENT_URI"))
            experimentUri = (String)System.getenv().get("HOBBIT_EXPERIMENT_URI");

        if(queryType<=0){
            Exception ex = new Exception("Query type was not specified");
            logger.error(ex.getMessage());
            throw ex;
        }


        sender2TaskGen = ExchangeDataSender.builder().queue(
                new ExchangeRabbitFactory(this.createConnection()),
                this.generateSessionQueueName(ACKNOWLEDGE_QUEUE_NAME)).build();

//        taskGenStatusRequestsReceiver = DataReceiverImpl.builder().dataHandler(new DataHandler() {
//            @Override
//            public void handleData(byte[] body){
//                if (body.length > 0) {
//                    String encryptedTaskId = new String(body);
//                    if(actualResponses.containsKey(encryptedTaskId)) {
//                        try {
//                            sender2TaskGen.sendData(encryptedTaskId.getBytes());
//                        } catch (IOException e) {
//                            logger.error("Could not sent duplicated notification");
//                        }
//                    }
//                    //logger.debug("Sensing duplicate notification: {}", shipId);
//
//                }
//            }
//        }).maxParallelProcessedMsgs(RECEIVER_THREADS).queue(this.getFactoryForIncomingDataQueues(), this.generateSessionQueueName(ACKNOWLEDGE_STATUS_QUEUE_NAME)).build();


//        encryptionKey = "encryptionKey."+exchangeQueueName;
//
//        exchangeChannel = this.cmdQueueFactory.getConnection().createChannel();
//        exchangeChannel.exchangeDeclare(exchangeQueueName, "fanout", false, true, (Map)null);

        logger.debug(new Date().toString()+" Init (queryType={}, experimentUri={}", queryType, experimentUri);

    }


    private void startTimer(){
        if(timerStarted)
            return;
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                timerLoop();
                //if(receivedExpectedCount>0 && receivedActual==receivedActual)
                //    timer.cancel();
            }
        }, 1000, timerPeriodSeconds*1000);
        timerStarted=true;
    }

    private synchronized void timerLoop(){
        long diffExpected = (expectedResponses.size() - lastReportedValue)/timerPeriodSeconds;
        long diffActual = (actualResponses.size() - lastReportedValue2)/timerPeriodSeconds;

        logger.debug("Actuals: {}, ({} t/s) {} Ignored: {}, Nofications sent: {}",
                actualResponses.size(),
                diffActual,
                (expectedResponses.size()>0?" Expectations: "+ expectedResponses.size() +" ("+diffExpected+" t/s)":""),
                ignoredTasks,
                actualResponses.size());
        lastReportedValue = expectedResponses.size();
        lastReportedValue2 = actualResponses.size();

//        if(sentBuffer.size()>0) {
//            logger.debug("Duplicating last notifications: " + sentBuffer.values().size());
//            for (String shipId : sentBuffer.keySet().toArray(new String[0])){
//                String encryptedTaskId = sentBuffer.get(shipId);
//                //lastSentTasksTimestamps.remove(enctyptedTaskId);
//                //lastSentTasks.remove(enctyptedTaskId);
//                //saveResultAndNotifyGenerator(enctyptedTaskId, timestamp, dataString);
//                try {
//                    sender2TaskGen.sendData(encryptedTaskId.getBytes());
//                } catch (IOException e) {
//                    logger.error("Could not sent duplicated notification");
//                }
//            }
//            //sentBuffer.clear();
//        }

    }

    @Override
    public void receiveExpectedResponseData(String encryptedTaskId, long timestamp, byte[] data) {
        logger.trace("receiveExpectedResponseData({})->...", encryptedTaskId);
        String dataString = new String(data);

//        if(dataString.equals("") || dataString.startsWith("null"))
//            return;

        try {
            String tripId=null;
            long orderingIndex=0;
            String expected="";
            if (queryType == 1) {
                String[] splitted = dataString.split(",");
                tripId = splitted[0];
                orderingIndex = Long.parseLong(splitted[1]);
                expected = tripId+","+String.join(",", Arrays.asList(splitted).subList(2, splitted.length));
            }else {
                tripId = dataString;
                orderingIndex = expectedResponses.size()+new Double(Math.random()*10000000).longValue();
                expected = dataString;
            }

            putTrip(orderingIndex, tripId, encryptedTaskId);
            putResult(true, encryptedTaskId, timestamp, expected);
        }
        catch (Exception e){
            logger.error("Error processing expected responce: "+e.getMessage());
        }

    }

    public synchronized void putTrip(long orderingIndex, String tripId, String encryptedTaskId){
        Map<Long, String> tripTuples = (tuplesPerTrips.containsKey(tripId)? tuplesPerTrips.get(tripId): new HashMap<>());
        tripTuples.put(orderingIndex, encryptedTaskId);
        tuplesPerTrips.put(tripId, tripTuples);
    }

    @Override
    public void receiveResponseData(String taskId, long timestamp, byte[] bytes) {
        String dataString = new String(bytes);
        logger.trace("receiveActualResponseData({})->{}",taskId, dataString);

        String[] splitted = taskId.split("_");
        String shipId = splitted[2];
        String encryptedTaskId = encryptTaskId(taskId, encryptionKey);

        if(actualResponses.containsKey(encryptedTaskId)) {
            ignoredTasks++;
            logger.warn("Result for the " + taskId + " was received earlier. Ignoring this for evaluation ({})", ignoredTasks);

        }else
            putResult(false, encryptedTaskId, timestamp, dataString);

        notifyGenerator(encryptedTaskId);
        sentBuffer.put(shipId, encryptedTaskId);

        lastSentTasks.put(encryptedTaskId, dataString);
        lastSentTasksTimestamps.put(encryptedTaskId, timestamp);

        //actualResponsesPairs.put(encryptedTaskId, new SerializableResult(timestamp, bytes));

    }

    public synchronized void putResult(boolean isExpectedResult, String taskId, long timestamp, String data){
        if (isExpectedResult) {
            expectedResponses.put(taskId, data);
            taskSentTimestamps.put(taskId, timestamp);
        } else {
            actualResponses.put(taskId, data);
            responseReceivedTimestamps.put(taskId, timestamp);
        }
    }

//    private void saveResultAndNotifyGenerator(String encryptedTaskId, long timestamp, String dataString){
//        logger.trace("saveResultAndNotifyGenerator({})", encryptedTaskId);
//
//        try{
//            putResult(false, encryptedTaskId, timestamp, dataString);
//            //sentBuffer.add(encryptedTaskId);
//            sender2TaskGen.sendData(encryptedTaskId.getBytes());
//
//
//        } catch (IOException e) {
////            lastSentTasks.put(encryptedTaskId, dataString);
////            lastSentTasksTimestamps.put(encryptedTaskId, timestamp);
//
//            //notificationsQueue.add(encryptedTaskId);
//            logger.error(new Date().toString()+" Notification failed "+ e.getMessage());
//        }
//
//
//    }

    private void notifyGenerator(String encryptedTaskId){
        logger.trace("NotifyGenerator({})", encryptedTaskId);

        try{
            sender2TaskGen.sendData(encryptedTaskId.getBytes());


        } catch (IOException e) {
//            lastSentTasks.put(encryptedTaskId, dataString);
//            lastSentTasksTimestamps.put(encryptedTaskId, timestamp);

            //notificationsQueue.add(encryptedTaskId);
            logger.error(new Date().toString()+" Notification failed "+ e.getMessage());
        }


    }


    @Override
    protected Iterator<ResultPair> createIterator(){
        logger.debug("createIterator()");
        List<ResultPair> ret = createResultPairs();
        return ret.iterator();
    }

    private synchronized List<ResultPair> createResultPairs(){
        //List<ResultPair> ret = new ArrayList<>();
        List<ResultPair> ret = Collections.synchronizedList(new ArrayList<>());
        List<String> debugRet = new ArrayList<>();
        int errors = 0;

        for(Map<Long, String> tripTuples : new ArrayList<>(tuplesPerTrips.values())){
            Long[] orderingIndexes = tripTuples.keySet().toArray(new Long[0]);
            Arrays.sort(orderingIndexes);
            for (long orderingIndex : orderingIndexes){

                try {
                    String encTaskId = tripTuples.get(orderingIndex);
                    Object taskSentTimeStamp = taskSentTimestamps.get(encTaskId);
                    String expectedResult = expectedResponses.get(encTaskId);
                    String actualResult = actualResponses.get(encTaskId);

                    if (expectedResult != null && taskSentTimeStamp != null && actualResult != null){

                        String debugLine = expectedResult + " <-> " + actualResult;
                        logger.trace((orderingIndex + 1) + " Adding: " + debugLine);
                        debugRet.add(debugLine);

                        long responseReceivedTimeStamp = responseReceivedTimestamps.get(encTaskId);
                        Result expected = new SerializableResult((long) taskSentTimeStamp, expectedResult.getBytes());
                        Result actual = new SerializableResult(responseReceivedTimeStamp, actualResult.getBytes());
                        ret.add(new ResultPairImpl(expected, actual));
                        //counter++;
                    } else {
                        errors++;
                    }
                } catch (Exception e) {
                    //logger.error("createResultPairs error"+ e.getMessage());
                    errors++;
                }
                //}
                //logger.debug(shipId);
            }
        }
        logger.debug("Iterator created({} pairs, {} expected, {} actuals, {} errors)", ret.size(), expectedResponses.size(), actualResponses.size(), errors);
        return ret;
    }

    @Override
    public void receiveCommand(byte command, byte[] data){
        if(command==Commands.TASK_GENERATOR_START_SIGNAL)
            startTimer();

        if(command==Commands.EVAL_STORAGE_TERMINATE){
            timer.cancel();
            try {
                startEvaluation();
            }
            catch (Exception e){
                logger.error("Evaluation failed");
                e.getCause();
            }
        }
        super.receiveCommand(command, data);
    }

    private void startEvaluation() throws Exception {
        logger.info("Start evaluation");

        this.sendToCmdQueue((byte)6);
        EvalModule evalModule = new EvalModule();
        evalModule.logger = logger;
        //evalModule.init();
        evalModule.queryType = queryType;

        logger.info("Creating result pairs");
        List<ResultPair> resultPairs = createResultPairs();
        for(ResultPair pair : resultPairs) {
            evalModule.evaluateResponse(pair.getExpected().getData(), pair.getActual().getData(), pair.getExpected().getSentTimestamp(), pair.getActual().getSentTimestamp());
        }

        Model model = evalModule.summarizeEvaluation(experimentUri);

        sendResultModel(model);
        sendEvalModuleTerminationCommand();

        logger.info("Evaluation done");
    }

    private void sendResultModel(Model model) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        model.write(outputStream, "JSONLD");
        sendToCmdQueue(EVAL_MODULE_FINISHED_SIGNAL, outputStream.toByteArray());
    }

    private void sendEvalModuleTerminationCommand() throws IOException {
        byte command = DOCKER_CONTAINER_TERMINATED;
        String finishedContainerName = "evalModule";
        byte exitCode = 0;
        byte[] data = RabbitMQUtils.writeByteArrays((byte[])null, new byte[][]{RabbitMQUtils.writeString(finishedContainerName)}, new byte[]{exitCode});
        sendToCmdQueue(command, data);
    }

    @Override
    public void close() throws IOException {

        IOUtils.closeQuietly(sender2TaskGen);
        try {
            super.close();
        } catch (Exception e) {

        }
        //System.exit(0);
        throw new IOException("Forcing container exit");

    }

}
