// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.scheduler.mesos.framework;

import java.io.IOException;
import java.util.logging.Logger;

import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosDriverFactory;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosJobFramework;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosTaskBuilder;
import com.twitter.heron.scheduler.mesos.framework.jobs.JobScheduler;
import com.twitter.heron.scheduler.mesos.framework.server.FrameworkHttpServer;
import com.twitter.heron.scheduler.mesos.framework.state.PersistenceStore;
import com.twitter.heron.scheduler.mesos.framework.state.ZkPersistenceStore;

public class FrameworkMain {
    private static final Logger LOG = Logger.getLogger(FrameworkMain.class.getName());

    public static void main(String[] args) throws IOException {
        // port would be the first argument
        int port = Integer.parseInt(args[0]);

        FrameworkConfiguration config = FrameworkConfiguration.getFrameworkConfiguration();
        config.schedulerName = args[1];
        config.master = args[2];
        config.user = args[3];
        config.failoverTimeoutSeconds = Integer.parseInt(args[4]);
        config.reconciliationIntervalInMs = Long.parseLong(args[5]);
        config.hostname = "";

        String zkConnectString = args[6];
        int connectionTimeoutMs = Integer.parseInt(args[7]);
        int sessionTimeoutMs = Integer.parseInt(args[8]);
        String zkRoot = args[9];

        PersistenceStore persistenceStore =
                new ZkPersistenceStore(zkConnectString, connectionTimeoutMs, sessionTimeoutMs, zkRoot);

        runScheduler(port, config, persistenceStore);
    }

    public static void runScheduler(int schedulerServerPort,
                                    FrameworkConfiguration config,
                                    PersistenceStore persistenceStore) throws IOException {

        MesosTaskBuilder mesosTaskBuilder = new MesosTaskBuilder();

        MesosJobFramework mesosScheduler = new MesosJobFramework(mesosTaskBuilder, persistenceStore, config);

        MesosDriverFactory mesosDriver = new MesosDriverFactory(mesosScheduler, persistenceStore, config);
        JobScheduler jobScheduler = new JobScheduler(mesosScheduler, persistenceStore, mesosDriver, config);

        // Start REST endpoint.
        LOG.info("Starting server on port: " + schedulerServerPort);
        runServer(jobScheduler, schedulerServerPort);

        // This would return immediately
        jobScheduler.start();
    }

    public static FrameworkHttpServer runServer(JobScheduler jobScheduler, int port) throws IOException {
        final FrameworkHttpServer schedulerFrameworkHttpServer = new FrameworkHttpServer(jobScheduler, port, true);
        schedulerFrameworkHttpServer.start();

        return schedulerFrameworkHttpServer;
    }
}
