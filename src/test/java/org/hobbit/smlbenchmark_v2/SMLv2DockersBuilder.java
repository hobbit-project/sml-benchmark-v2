package org.hobbit.smlbenchmark_v2;


import org.hobbit.sdk.docker.builders.DynamicDockerFileBuilder;
import org.hobbit.sdk.utils.ComponentStarter;

import static org.hobbit.smlbenchmark_v2.Constants.PROJECT_NAME;


/**
 * @author Pavel Smirnov
 */

public class SMLv2DockersBuilder extends DynamicDockerFileBuilder {

    public SMLv2DockersBuilder(Class runnerClass) throws Exception {
        super("SMLv2DockersBuilder");
        jarFilePath("target/sml-benchmark-2-1.0-SNAPSHOT.jar");
        buildDirectory(".");
        dockerWorkDir("/usr/src/"+ PROJECT_NAME);
        containerName(runnerClass.getSimpleName());
        runnerClass(ComponentStarter.class, runnerClass);
    }

}
