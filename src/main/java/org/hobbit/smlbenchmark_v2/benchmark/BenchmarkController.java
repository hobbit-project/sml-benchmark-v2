package org.hobbit.smlbenchmark_v2.benchmark;

import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.hobbit.sdk.JenaKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

import static org.hobbit.smlbenchmark_v2.Constants.*;


/**
 * @author Pavel Smirnov
 */

public class BenchmarkController extends AbstractBenchmarkController {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkController.class);
    private static JenaKeyValue experimentModel = new JenaKeyValue();
    private long timeoutStarted;
    private int queryType = -1;
    private int dataFormat = 0;
    private int generatorsCount = 1;
    private long tuplesLimit = 0;
    private int timeoutMin = 60;
    private Timer timer;
    private Semaphore taskGenGenerationFinishedMutex;


    @Override
    public void init() throws Exception {
        super.init();
        logger.debug("Init()");
        // Your initialization code comes here...

        taskGenGenerationFinishedMutex = new Semaphore(0);

        experimentModel = new JenaKeyValue.Builder().buildFrom(benchmarkParamModel);
        logger.debug("BenchmarkModel: "+ experimentModel.encodeToString());

        queryType = experimentModel.getIntValueFor(QUERY_TYPE_KEY);

        if(experimentModel.containsKey(TUPLES_LIMIT))
            tuplesLimit = experimentModel.getIntValueFor(TUPLES_LIMIT);

        if(experimentModel.containsKey(DATA_FORMAT_KEY))
            dataFormat = experimentModel.getIntValueFor(DATA_FORMAT_KEY);

        if(experimentModel.containsKey(BENCHMARK_TIMEOUT_MIN) && experimentModel.getIntValueFor(BENCHMARK_TIMEOUT_MIN)>0)
            timeoutMin = experimentModel.getIntValueFor(BENCHMARK_TIMEOUT_MIN);


        if(experimentModel.containsKey(GENERATORS_COUNT))
            generatorsCount = experimentModel.getIntValueFor(GENERATORS_COUNT);

        if(queryType<=0){
            Exception ex = new Exception("Query type is not specified correctly");
            logger.error(ex.getMessage());
            throw ex;
        }
        // Create the other components

        // Create data generators

        String[] envVariables = new String[]{
                "HOBBIT_EXPERIMENT_URI="+this.experimentUri,
                "QUERY_TYPE="+String.valueOf(queryType),
                "DATA_FORMAT="+String.valueOf(dataFormat),
                "TUPLES_LIMIT="+String.valueOf(tuplesLimit)
        };

        logger.debug("Gen.EnvVariables: "+String.join(", ",envVariables));
        //String[] envVariables = experimentModel.mapToArray();
//        logger.debug("createDataGenerators({})",numberOfDataGenerators);
        //createDataGenerators(DATAGEN_IMAGE_NAME, numberOfDataGenerators, envVariables);


        // Create task generators

        logger.debug("createTaskGenerators({})", generatorsCount);
        createTaskGenerators(TASKGEN_IMAGE_NAME, generatorsCount, envVariables);

        // Create evaluation storage
        createEvaluationStorage(EVAL_STORAGE_IMAGE_NAME, envVariables);

        waitForComponentsToInitialize();

        logger.info("Benchmark is ready. Waiting for the system init");
        timeoutStarted = new Date().getTime();
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if(new Date().getTime()- timeoutStarted >timeoutMin*60000){
                    logger.error("Timeout reached. Stopping benchmark");
                    System.exit(1);
                }
            }
        }, 1000, 5000);

    }


    @Override
    public void receiveCommand(byte command, byte[] data){
        if(command==Commands.TASK_GENERATION_FINISHED)
            taskGenGenerationFinishedMutex.release();

        super.receiveCommand(command, data);
    }

    @Override
    protected void executeBenchmark() throws Exception {
        logger.debug("executeBenchmark(sending TASK_GENERATOR_START_SIGNAL & DATA_GENERATOR_START SIGNAL)");
        // give the start signals
        sendToCmdQueue(Commands.TASK_GENERATOR_START_SIGNAL);

        timeoutStarted = new Date().getTime();

        logger.debug("Waiting task generator to finish data generation");
        taskGenGenerationFinishedMutex.acquire();

        logger.debug("waitForSystemToFinish() to finish to send DATA_GENERATION_FINISHED");
        waitForSystemToFinish();

        sendToCmdQueue(SYSTEM_FINISHED_SIGNAL);

        logger.debug("waitForTaskGenToFinish() to finish to send EVAL_STORAGE_TERMINATE SIGNAL");
        waitForTaskGenToFinish();

        sendToCmdQueue(Commands.EVAL_STORAGE_TERMINATE);

        waitForEvalComponentsToFinish();

        sendResultModel(resultModel);

        timer.cancel();
    }


    @Override
    protected void waitForEvalComponentsToFinish() {

        logger.debug("Waiting for the evaluation storage to finish.");

        try {
            this.evalStoreTerminatedMutex.acquire();
        } catch (InterruptedException var3) {
            String errorMsg = "Interrupted while waiting for the evaluation storage to terminate.";
            logger.error(errorMsg);
            throw new IllegalStateException(errorMsg, var3);
        }
    }


    @Override
    public void close() throws IOException {
        logger.debug("close()");
        // Free the resources you requested here

        // Always close the super class after yours!
        super.close();
    }

}
