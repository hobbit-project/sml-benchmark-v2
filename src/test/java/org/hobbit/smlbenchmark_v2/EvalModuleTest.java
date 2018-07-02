package org.hobbit.smlbenchmark_v2;

import com.rabbitmq.client.MessageProperties;
import org.apache.jena.rdf.model.Model;
import org.hobbit.core.Commands;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sdk.EnvironmentVariablesWrapper;
import org.hobbit.sdk.utils.CommandQueueListener;
import org.hobbit.sdk.utils.CommandSender;
import org.hobbit.smlbenchmark_v2.benchmark.EvalModule;
import org.junit.Test;

import static org.hobbit.sdk.CommonConstants.EXPERIMENT_URI;
import static org.hobbit.smlbenchmark_v2.SMLBenchmarkV2Test.createBenchmarkParameters;

public class EvalModuleTest extends EnvironmentVariablesWrapper {

    @Test
    public void checkModel() throws Exception {

        setupCommunicationEnvironmentVariables("rabbit", "0");
        setupBenchmarkEnvironmentVariables(EXPERIMENT_URI, createBenchmarkParameters());
        environmentVariables.set("QUERY_TYPE","1");

        EvalModule evalModule = new EvalModule();
        evalModule.init();
        Model model = evalModule.summarizeEvaluation("Exp1");
        while (true) {
            new CommandSender(Commands.BENCHMARK_FINISHED_SIGNAL, RabbitMQUtils.writeModel(model)).send();
        }

    }
}
