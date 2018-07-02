package org.hobbit.smlbenchmark_v2;

import org.hobbit.core.Commands;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;

public class Constants {
    public static String ENCRYPTION_KEY_NAME = "ENCRYPTION_KEY";
    public static final String DATASET_FILE_NAME_KEY = "DATASET_FILE_NAME";
//
    public static final String GIT_REPO_PATH = "git.project-hobbit.eu:4567/smirnp/";
    public static final String PROJECT_NAME = "sml-benchmark-v2";
    //public static final String PROJECT_NAME = "sml-benchmark-v2-mt";

//    public static final String GIT_REPO_PATH = "git.project-hobbit.eu:4567/";
//    public static final String PROJECT_NAME = "debs_2018_gc";

    //public static final String GIT_REPO_PATH = "";

    public static final String DATASET_FILE_NAME = "data/debs2018_complete_fixed_3_labeled_v8_training.csv";

    public static final byte SYSTEM_FINISHED_SIGNAL = 19;

    public static final String BENCHMARK_IMAGE_NAME = GIT_REPO_PATH+ PROJECT_NAME +"/benchmark-controller";
    public static final String SYSTEM_IMAGE_NAME = GIT_REPO_PATH+ PROJECT_NAME +"/system-adapter";

    public static final String DATAGEN_IMAGE_NAME = GIT_REPO_PATH+ PROJECT_NAME +"/data-generator";
    public static final String TASKGEN_IMAGE_NAME = GIT_REPO_PATH+ PROJECT_NAME +"/task-generator";
    public static final String EVAL_STORAGE_IMAGE_NAME = GIT_REPO_PATH+ PROJECT_NAME +"/eval-storage";
    public static final String EVALMODULE_IMAGE_NAME = GIT_REPO_PATH+ PROJECT_NAME +"/eval-module";

    public static final String ACKNOWLEDGE_QUEUE_NAME = "hobbit.sml2.ack";
    public static final String ACKNOWLEDGE_STATUS_QUEUE_NAME = "hobbit.sml2.ack_status";

    public static final Charset CHARSET = Charset.forName("UTF-8");

    public static final String SYSTEM_URI = "http://project-hobbit.eu/sml-benchmark-v2/sampleSystem";

    public static final SimpleDateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("dd-MM-yy HH:mm");
    public static final String BENCHMARK_URI = "http://project-hobbit.eu/sml-benchmark-v2";

    public static final String GENERATORS_COUNT = BENCHMARK_URI+"/generatorsCount";
    public static final String TUPLES_LIMIT = BENCHMARK_URI+"/tuplesLimit";
    public static final String BENCHMARK_TIMEOUT_MIN = BENCHMARK_URI+"/benchmarkTimeoutMin";

    public static final String QUERY_TYPE_KEY = BENCHMARK_URI+"/queryType";
    public static final String DATA_FORMAT_KEY = BENCHMARK_URI+"/dataFormat";
    public static final String CONTAINERS_COUNT_KEY = BENCHMARK_URI+"/containersCount";

    public static final String KPI_AVG_LATENCY_KEY = BENCHMARK_URI+"/averageLatencyMs";
    public static final String KPI_EVALUATED_PAIRS_KEY = BENCHMARK_URI+"/evaluatedPairsCount";
    public static final String KPI_SYSTEM_WORKING_TIME_KEY = BENCHMARK_URI+"/systemWorkingTimeSeconds";
    public static final String KPI_AVG_EARLYNESS_RATE_KEY = BENCHMARK_URI+"/averageEarlynessRate";
    public static final String KPI_MEAN_ERROR_KEY = BENCHMARK_URI+"/meanErrorMin";
    public static final String KPI_TOTAL_RANK_KEY = BENCHMARK_URI+"/totalRank";

    public static final int RECEIVER_THREADS = 1;

}
