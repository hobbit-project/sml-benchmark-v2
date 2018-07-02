package org.hobbit.smlbenchmark_v2;

import org.hobbit.smlbenchmark_v2.benchmark.generator.TaskGenerator;
import org.hobbit.smlbenchmark_v2.utils.Dataset;
import org.hobbit.smlbenchmark_v2.utils.Trip;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Map;

import static org.hobbit.smlbenchmark_v2.Constants.DATASET_FILE_NAME;

public class MultipleGeneratorsTest {

    @Ignore
    @Test
    public void balancingAlgTest() throws Exception {


        TaskGenerator generator = new TaskGenerator();


        //String[] lines = Utils.readFile(Paths.get("data","debs2018_training_labeled.csv"), recordsLimit);
        Dataset dataset = new Dataset(null);
        String[] lines = dataset.readFile(Paths.get(DATASET_FILE_NAME), 0);
        Map<String, Map<String, Trip>> tripsPerShips = dataset.getTripsPerShips(lines);
        generator.balanceShipsAmongGenerators(tripsPerShips,3);

//        generator.init();
//        generator.generateData();

    }

}
