/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.connectors.flink.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.util.FlinkException;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.net.URI;

public class FlinkMiniClusterWithSavepointCommand extends LocalFlinkMiniCluster {

    public FlinkMiniClusterWithSavepointCommand(Configuration userConfiguration) {
        super(userConfiguration);
    }

    public String triggerSavepoint(JobID jobId, URI targetPath) throws Exception {
        final String target = new Path(targetPath).toString();
        final FiniteDuration timeout = timeout();
        
        final ActorGateway jobManagerGateway = getLeaderGateway(timeout);
        
        Future<Object> savepointPathFuture = jobManagerGateway.ask(
                new JobManagerMessages.TriggerSavepoint(jobId, Option.apply(target)), timeout);

        Object result = Await.result(savepointPathFuture, timeout);
        if (result instanceof JobManagerMessages.TriggerSavepointSuccess) {
            return ((JobManagerMessages.TriggerSavepointSuccess) result).savepointPath();
        } else if (result instanceof JobManagerMessages.TriggerSavepointFailure) {
            throw new SavepointFailedException(((JobManagerMessages.TriggerSavepointFailure) result).cause());
        } else {
            throw new Exception("Unexpected response: " + result);
        }
    }

    public void cancelJob(JobID jobId) throws Exception {
        // this needs better support in the Flink Mini Cluster
        if (getCurrentlyRunningJobsJava().contains(jobId)) {
            final FiniteDuration timeout = timeout();
            final ActorGateway jobManagerGateway = getLeaderGateway(timeout);

            Future<Object> cancelFuture = jobManagerGateway.ask(
                    new JobManagerMessages.CancelJob(jobId), timeout);

            Object result = Await.result(cancelFuture, timeout);
            if (!(result instanceof JobManagerMessages.CancellationSuccess)) {
                throw new Exception("Cancellation failed");
            }
        } else {
            throw new IllegalStateException("Job is not running");
        }
    }

    // ------------------------------------------------------------------------

    public static FlinkMiniClusterWithSavepointCommand create(int numTaskSlots) {
        final Configuration config = new Configuration();

        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numTaskSlots);

        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false);

        return new FlinkMiniClusterWithSavepointCommand(config);
    }

    // ------------------------------------------------------------------------

    public static class SavepointFailedException extends FlinkException {


        public SavepointFailedException(Throwable cause) {
            super(cause);
        }
    }
    
}
