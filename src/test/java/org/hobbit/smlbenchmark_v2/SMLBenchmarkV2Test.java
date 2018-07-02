package org.hobbit.smlbenchmark_v2;

import org.hobbit.core.components.Component;
import org.hobbit.sdk.EnvironmentVariablesWrapper;
import org.hobbit.sdk.JenaKeyValue;
import org.hobbit.sdk.docker.MultiThreadedImageBuilder;
import org.hobbit.sdk.docker.RabbitMqDockerizer;
import org.hobbit.sdk.docker.builders.PullBasedDockersBuilder;
import org.hobbit.sdk.docker.builders.hobbit.*;
import org.hobbit.sdk.utils.CommandQueueListener;
import org.hobbit.sdk.utils.ComponentsExecutor;
import org.hobbit.sdk.utils.commandreactions.MultipleCommandsReaction;
import org.hobbit.smlbenchmark_v2.benchmark.*;
import org.hobbit.smlbenchmark_v2.benchmark.EvalStorage;
import org.hobbit.smlbenchmark_v2.benchmark.generator.TaskGenerator;
import org.hobbit.smlbenchmark_v2.system.SystemAdapter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;

import static org.hobbit.core.Constants.BENCHMARK_PARAMETERS_MODEL_KEY;
import static org.hobbit.core.Constants.SYSTEM_PARAMETERS_MODEL_KEY;
import static org.hobbit.sdk.CommonConstants.*;
import static org.hobbit.smlbenchmark_v2.Constants.*;

/**
 * @author Pavel Smirnov
 */

public class SMLBenchmarkV2Test extends EnvironmentVariablesWrapper {

    private RabbitMqDockerizer rabbitMqDockerizer;
    private ComponentsExecutor componentsExecutor;
    private CommandQueueListener commandQueueListener;


    BenchmarkDockerBuilder benchmarkBuilder;
    TaskGenDockerBuilder taskGeneratorBuilder;
    EvalStorageDockerBuilder evalStorageBuilder;
    SystemAdapterDockerBuilder systemAdapterBuilder;
    EvalModuleDockerBuilder evalModuleBuilder;


    public void init(Boolean useCachedImages, Boolean useCachedContainers) throws Exception {

        rabbitMqDockerizer = RabbitMqDockerizer.builder().build();

        setupCommunicationEnvironmentVariables(rabbitMqDockerizer.getHostName(), "session_"+String.valueOf(new Date().getTime()));
        setupBenchmarkEnvironmentVariables(EXPERIMENT_URI, createBenchmarkParameters());
        //setupGeneratorEnvironmentVariables(1,1);
        setupSystemEnvironmentVariables(SYSTEM_URI, createSystemParameters());

        benchmarkBuilder = new BenchmarkDockerBuilder(new SMLv2DockersBuilder(BenchmarkController.class).imageName(BENCHMARK_IMAGE_NAME).useCachedImage(useCachedImages).useCachedContainer(useCachedContainers));
        taskGeneratorBuilder = new TaskGenDockerBuilder(new SMLv2DockersBuilder(TaskGenerator.class).imageName(TASKGEN_IMAGE_NAME).useCachedImage(useCachedImages).addFileOrFolder(DATASET_FILE_NAME).useCachedContainer(useCachedContainers));

        evalStorageBuilder = new EvalStorageDockerBuilder(new SMLv2DockersBuilder(EvalStorage.class).imageName(EVAL_STORAGE_IMAGE_NAME).useCachedImage(useCachedImages).useCachedContainer(useCachedContainers));


        //systemAdapterBuilder = new SystemAdapterDockerBuilder(new PullBasedDockersBuilder(systemURL).useCachedContainer(false));
        systemAdapterBuilder = new SystemAdapterDockerBuilder(new SMLv2DockersBuilder(SystemAdapter.class).imageName(SYSTEM_IMAGE_NAME).useCachedImage(useCachedImages).addFileOrFolder(DATASET_FILE_NAME).useCachedContainer(useCachedContainers));

        evalModuleBuilder = new EvalModuleDockerBuilder(new SMLv2DockersBuilder(EvalModule.class).imageName(EVALMODULE_IMAGE_NAME).useCachedImage(useCachedImages).useCachedContainer(useCachedContainers));
    }

    @Test
    @Ignore
    public void buildImages() throws Exception {

        long started = new Date().getTime();
        init(false, false);

        //evalStorageBuilder.build().prepareImage();
        MultiThreadedImageBuilder builder = new MultiThreadedImageBuilder(4);
        //builder.addTask(benchmarkBuilder);
        builder.addTask(taskGeneratorBuilder);
        //builder.addTask(evalStorageBuilder);
        //builder.addTask(systemAdapterBuilder);
        builder.build();

    }

    @Test
    public void checkHealth() throws Exception {
        checkHealth(false);
    }

    @Test
    public void checkHealthDockerized() throws Exception {
        checkHealth(true);
    }

    private void checkHealth(Boolean dockerize) throws Exception {

        Boolean useCachedImages = true;
        Boolean useCachedContainer = false;

        init(useCachedImages, useCachedContainer);

        Component benchmarkController = new BenchmarkController();
        Component taskGen = new TaskGenerator();
        Component evalStorage = new EvalStorage();

        Component systemAdapter = new SystemAdapter();

        if(dockerize){
            //benchmarkController = benchmarkBuilder.build();
            taskGen = taskGeneratorBuilder.build();
            evalStorage = evalStorageBuilder.build();
            systemAdapter = systemAdapterBuilder.build();
        }

        commandQueueListener = new CommandQueueListener();
        componentsExecutor = new ComponentsExecutor();

        rabbitMqDockerizer.run();

        commandQueueListener.setCommandReactions(
                new MultipleCommandsReaction.Builder(componentsExecutor, commandQueueListener)
                        .benchmarkController(benchmarkController).benchmarkControllerImageName(BENCHMARK_IMAGE_NAME)
                        .taskGenerator(taskGen).taskGeneratorImageName(TASKGEN_IMAGE_NAME)
                        .evalStorage(evalStorage).evalStorageImageName(EVAL_STORAGE_IMAGE_NAME)
                        .systemAdapter(systemAdapter).systemAdapterImageName(SYSTEM_IMAGE_NAME)
                        .build()
        );

        componentsExecutor.submit(commandQueueListener);
        commandQueueListener.waitForInitialisation();

        commandQueueListener.submit(BENCHMARK_IMAGE_NAME, new String[]{ BENCHMARK_PARAMETERS_MODEL_KEY+"="+ createBenchmarkParameters() });
        commandQueueListener.submit(SYSTEM_IMAGE_NAME, new String[]{ SYSTEM_PARAMETERS_MODEL_KEY+"="+ createSystemParameters() });


        commandQueueListener.waitForTermination();
        commandQueueListener.terminate();
        componentsExecutor.shutdown();

        //rabbitMqDockerizer.stop();

        Assert.assertFalse(componentsExecutor.anyExceptions());
    }

    private static int QUERY_TYPE = 2;
    private static int GENERATORS_COUNT = 1;

    public static String createBenchmarkParameters(){
        JenaKeyValue kv = new JenaKeyValue(EXPERIMENT_URI);
        //kv.setValue(TUPLES_LIMIT, 72);
        //kv.setValue(TUPLES_LIMIT, 316);
        //kv.setValue(TUPLES_LIMIT, 44);
        //kv.setValue(TUPLES_LIMIT, 350000);
        kv.setValue(TUPLES_LIMIT, 3000);
        //kv.setValue(TUPLES_LIMIT, 100000);
        kv.setValue(QUERY_TYPE_KEY, QUERY_TYPE);
        kv.setValue(DATA_FORMAT_KEY, 0);

        kv.setValue(BENCHMARK_TIMEOUT_MIN, 15);
        kv.setValue(Constants.GENERATORS_COUNT, GENERATORS_COUNT);
        kv.setValue(CONTAINERS_COUNT_KEY, GENERATORS_COUNT);
        return kv.encodeToString();
    }

    public static String createSystemParameters(){
        JenaKeyValue kv = new JenaKeyValue();
        kv.setValue(QUERY_TYPE_KEY, QUERY_TYPE);
        kv.setValue(CONTAINERS_COUNT_KEY, GENERATORS_COUNT);
        return kv.encodeToString();
    }


}
