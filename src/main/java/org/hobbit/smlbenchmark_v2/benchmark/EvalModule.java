package org.hobbit.smlbenchmark_v2.benchmark;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.core.components.AbstractEvaluationModule;
import org.hobbit.sdk.KeyValue;
import org.hobbit.vocab.HOBBIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import static org.hobbit.core.Commands.*;
import static org.hobbit.smlbenchmark_v2.Constants.*;


/**
 * @author Pavel Smirnov
 */

public class EvalModule extends AbstractEvaluationModule {
    public Logger logger = LoggerFactory.getLogger(EvalModule.class);
    private int evaluatedPairsCount = 0;
    public int queryType = -1;
    private long totalLatency = 0;

    long firstTaskTimestamp = Long.MAX_VALUE;
    long lastResponseTimestamp=0;
    //List<Object[]> lastCorrectSeqLengths = new ArrayList<>();
    //Map<String, List<KeyValue>> resultPairsPerTrip = new LinkedHashMap<>();
    //List<List<KeyValue>> resultPairsPerTrip = new ArrayList();
    Map<String, List<KeyValue>> resultPairsPerTrip = new HashMap<>();
    int responseErrors=0;
    Map<String, Long> tripDurations = new HashMap<>();

    long correctResults=0;
    double totalMeanError = 0;
    Timer timer;
    long lastReportedEvaluatedPairsCount = 0;
    public String publicExperimentUri;

    @Override
    public void init() throws Exception {
        super.init();

        if(System.getenv().containsKey("QUERY_TYPE"))
            queryType = Integer.parseInt(System.getenv().get("QUERY_TYPE"));

        if(queryType<=0){
            Exception ex = new Exception("Query type is not specified correctly");
            logger.error(ex.getMessage());
            throw ex;
        }
        publicExperimentUri = experimentUri;
    }

    @Override
    public void run() throws Exception {
        this.sendToCmdQueue((byte)6);
        startTimer();
        this.collectResponses();
        timer.cancel();
        Model model = this.summarizeEvaluation();
        logger.info("The result model has " + model.size() + " triples.");
        this.sendResultModel(model);
    }

    private void startTimer(){
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {

            long valDiff = evaluatedPairsCount - lastReportedEvaluatedPairsCount;
            logger.debug("{} pairs evaluated. Curr: {} tuples/s", evaluatedPairsCount, valDiff);
            lastReportedEvaluatedPairsCount = evaluatedPairsCount;

            }
        }, 1000, 1000);
    }

    @Override
    protected void evaluateResponse(byte[] expectedData, byte[] actualData, long taskSentTimestamp, long responseReceivedTimestamp) throws Exception {
        // evaluate the given response and store the result, e.g., increment internal counters
        String expected = new String(expectedData);

        String actual = new String(actualData);
        logger.trace("evaluateResponse(exp={}, act={})", expected, actual);


        firstTaskTimestamp = Math.min(firstTaskTimestamp, taskSentTimestamp);
        lastResponseTimestamp = Math.max(lastResponseTimestamp, responseReceivedTimestamp);

//        if(expected.equals("") || expected.startsWith("null"))
//            return;

        try {

            if (queryType == 1)
                evaluateResponseQ1(expected, actual);
            else
                evaluateResponseQ2(expected, actual);

        }catch (Exception e){
            logger.trace("evaluateResponse error: actual value -> {}", actual);
            responseErrors++;
        }

        long latency = responseReceivedTimestamp-taskSentTimestamp;
        totalLatency += latency;


        String test="123";
    }

    private void evaluateResponseQ1(String expected, String actual) throws ParseException {
        String[] splitted = expected.split(",");

        String tripId = splitted[0];
        String tupleTimestamp = splitted[1];
        if(splitted.length==3) {
            String portName = splitted[2];

            List<KeyValue> tripResultPairs = (resultPairsPerTrip.containsKey(tripId)? resultPairsPerTrip.get(tripId): new ArrayList<KeyValue>()) ;
            KeyValue kv = new KeyValue();
            kv.setValue("timestamp", tupleTimestamp);
            kv.setValue("expected", portName);
            kv.setValue("actual", actual);
            tripResultPairs.add(kv);
            resultPairsPerTrip.put(tripId, tripResultPairs);
        }

    }

    private void evaluateResponseQ2(String expected, String actual) throws ParseException {
        String [] splittedExpected = expected.split(",");
        //Long tripDuration = Long.parseLong(splittedExpected[0]);
        if(splittedExpected.length>0){
            //String expectedPortName = splittedExpected[0];
            Date expectedDate = DEFAULT_DATE_FORMAT.parse(splittedExpected[1]);

            //String[] splittedActual = actual.split(",");
            //String actualPortName = splittedActual[0];
            //if (actualPortName.equals(expectedPortName)){
            //correctResults++;
            Date actualDate = null;
            try {
                actualDate = DEFAULT_DATE_FORMAT.parse(actual);
            } catch (Exception e) {

            }
            if(actualDate==null)
                try {
                    actualDate = DEFAULT_DATE_FORMAT.parse(actual.split(",")[1]);
                } catch (Exception e) {

                }

            if(actualDate!=null) {
                totalMeanError += (Math.abs(expectedDate.getTime() - actualDate.getTime())) / 60000;
                evaluatedPairsCount++;
            }else
                logger.error("Cound not parse actual date: "+actual);
        }
    }

    @Override
    protected Model summarizeEvaluation() throws Exception {
        return summarizeEvaluation(experimentUri);
    }

    public Model summarizeEvaluation(String experiment_uri) throws Exception {
        logger.debug("summarizeEvaluation()");
        long avgLatency = (evaluatedPairsCount >0? Math.round(totalLatency*1000.0 / evaluatedPairsCount)/1000 : 0 );
        long systemWorkingTimeMs = lastResponseTimestamp-firstTaskTimestamp;

        double workingTimeS = Math.round(systemWorkingTimeMs)/1000;
        double earlynessRatio = -1;
        double meanError = -1.0;
        double totalRank = 0.0;
        if(queryType==1) {
            earlynessRatio = summarizeQuery1();
            earlynessRatio = Math.floor(earlynessRatio*1000.0)/1000;
            totalRank = earlynessRatio*0.75 + workingTimeS*0.25;
        }else {
            if(evaluatedPairsCount>0){
                meanError = totalMeanError / evaluatedPairsCount;
                meanError = Math.floor(meanError * 1000.0) / 1000;
            }else
                meanError = 99999999999.0;
            totalRank = (1/meanError)*0.75 + workingTimeS*0.25;
        }
        //double rankA = (queryType==1 ? earlynessRatio: 1-meanError);
        //double rankB = 1-systemWorkingTimeMs/3600000.0;

        //double rank = 0.75 * rankA + 0.25 * rankB;
        //rank = Math.round(rank*100000.0)/1000.0;




        logger.debug("Evaluation statistics (QueryType={}): {} response errors", queryType, responseErrors);
        logger.debug("{} = {}",KPI_AVG_LATENCY_KEY, avgLatency);
        logger.debug("{} = {}",KPI_SYSTEM_WORKING_TIME_KEY, workingTimeS);
        logger.debug("{} = {}",KPI_EVALUATED_PAIRS_KEY, evaluatedPairsCount);
        logger.debug("{} = {}",KPI_AVG_EARLYNESS_RATE_KEY, earlynessRatio);
        logger.debug("{} = {}",KPI_MEAN_ERROR_KEY, meanError);


        // All tasks/responsens have been evaluated. Summarize the results,
        // write them into a Jena model and send it to the benchmark controller.

        Model model = createModel(experiment_uri);
        Resource experimentResource = model.getResource(experiment_uri);
        model.add(experimentResource , RDF.type, HOBBIT.Experiment);
        model.add(experimentResource, model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), model.getResource("http://w3id.org/hobbit/vocab#Experiment"));

        model.add(experimentResource, model.getProperty(KPI_AVG_LATENCY_KEY), model.createTypedLiteral(avgLatency));
        model.add(experimentResource, model.getProperty(KPI_SYSTEM_WORKING_TIME_KEY), model.createTypedLiteral(workingTimeS));
        model.add(experimentResource, model.getProperty(KPI_EVALUATED_PAIRS_KEY), model.createTypedLiteral(evaluatedPairsCount));
        model.add(experimentResource, model.getProperty(KPI_AVG_EARLYNESS_RATE_KEY), model.createTypedLiteral(earlynessRatio));
        model.add(experimentResource, model.getProperty(KPI_MEAN_ERROR_KEY), model.createTypedLiteral(meanError));

        logger.debug(model.toString());

        return model;
    }

    public double summarizeQuery1() throws Exception {
        List<KeyValue> summary = new ArrayList<>();
        for(List<KeyValue> tripPairs: resultPairsPerTrip.values()){
            int lastCorrectStartingTupleIndex = -1;
            int tripTuplesIndex=0;

            String exptectedValue=null;
            String prevActual=null;
            for(KeyValue pair : tripPairs){
                try {
                    exptectedValue = pair.getStringValueFor("expected");
                    String actualValue = pair.getStringValueFor("actual");
                    if(exptectedValue.equals(actualValue)){
                        correctResults++;
                        if(!actualValue.equals(prevActual))
                            lastCorrectStartingTupleIndex = tripTuplesIndex;

                    }else
                        lastCorrectStartingTupleIndex = tripTuplesIndex;

                    prevActual = actualValue;
                    evaluatedPairsCount++;
                } catch (Exception e) {
                    logger.error("Could not get value: "+e.getMessage());
                }
                tripTuplesIndex++;
            }

            //long tripDuration = -1;
            //long lastCorrectStartingTupleOffset = -1;
            long tripStartTimestamp = -1;
            long tripEndTimestamp = -1;
            long lastCorrectStartingTupleTimestamp = -1;
            try {

                Date tripStarts = DEFAULT_DATE_FORMAT.parse(tripPairs.get(0).getStringValueFor("timestamp"));
                Date tripEnds = DEFAULT_DATE_FORMAT.parse(tripPairs.get(tripPairs.size()-1).getStringValueFor("timestamp"));
                //tripDuration = tripEnds.getTime()-tripStarts.getTime();
                tripStartTimestamp = tripStarts.getTime();
                tripEndTimestamp = tripEnds.getTime();

                Date timestamp = DEFAULT_DATE_FORMAT.parse(tripPairs.get(lastCorrectStartingTupleIndex).getStringValueFor("timestamp"));
                lastCorrectStartingTupleTimestamp = timestamp.getTime();
                //lastCorrectStartingTupleOffset = tripEnds.getTime()-timestamp.getTime();
            }
            catch (Exception e){
                logger.error("Failed to calculate trip duration: "+e.getMessage());
            }


            KeyValue tripSummary = new KeyValue();
            tripSummary.put("lastCorrectStartingTupleIndex", lastCorrectStartingTupleIndex);
            tripSummary.put("lastCorrectStartingTupleTimestamp", lastCorrectStartingTupleTimestamp);
            tripSummary.put("tripStartTimestamp", tripStartTimestamp);
            tripSummary.put("tripEndTimestamp", tripEndTimestamp);
            tripSummary.put("tripPairsCount", tripTuplesIndex);
            summary.add(tripSummary);
            //ret.put(exptectedValue+"_"+tripEndTimestamp, tripSummary);
            //logger.debug("{} -> {}", tripId, result.toJSONString());
        }

        double cumulativeCorrectSeqOffsetLength = 0;
        double cumulativeCorrectSeqLength = 0;
        for (KeyValue trip : summary){
            long tripStartTimestamp = trip.getLongValueFor("tripStartTimestamp");
            long tripEndTimestamp = trip.getLongValueFor("tripEndTimestamp");
            long tripDuration = tripEndTimestamp-tripStartTimestamp;

            if(tripDuration>0) {
                int tripPairsCount = trip.getIntValueFor("tripPairsCount");
                int lastCorrectStartingTupleIndex = trip.getIntValueFor("lastCorrectStartingTupleIndex");
                long lastCorrectStartingTupleTimestamp = trip.getLongValueFor("lastCorrectStartingTupleTimestamp");
                long lastCorrectTupleOffsetFromEnd = tripEndTimestamp - lastCorrectStartingTupleTimestamp;

                cumulativeCorrectSeqLength += (tripPairsCount - lastCorrectStartingTupleIndex) * 1.0 / tripPairsCount;
                cumulativeCorrectSeqOffsetLength += lastCorrectTupleOffsetFromEnd * 1.0 / tripDuration;
                //cumulativeCorrectSeqOffsetLength += tripDuration*1.0/(lastCorrectStartingTupleOffset==0?1:lastCorrectStartingTupleOffset);
                //cumulativeCorrectSeqOffsetLength += tripStartTimestamp*1.0/lastCorrectStartingTupleTimestamp;
                String test = "123";
            }
        }
        //earlynessRatio = cumulativeEarlynessRatio / lastCorrectSeqLengths.size();
        //earlynessRatio = cumulativeCorrectSeqLength;
        double earlynessRatio = cumulativeCorrectSeqOffsetLength/summary.size();
        return earlynessRatio;
    }

    public Model createModel(String experiment_uri) {
        Model resultModel = ModelFactory.createDefaultModel();
        resultModel.add(resultModel.createResource(experiment_uri), RDF.type, HOBBIT.Experiment);
        return resultModel;
    }

    private void sendResultModel(Model model) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        model.write(outputStream, "JSONLD");
        sendToCmdQueue(EVAL_MODULE_FINISHED_SIGNAL, outputStream.toByteArray());
    }


}
