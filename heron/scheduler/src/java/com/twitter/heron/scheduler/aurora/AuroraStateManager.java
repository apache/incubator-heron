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

package com.twitter.heron.scheduler.aurora;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.util.ShellUtility;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.statemgr.WatchCallback;
import com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager;

/**
 * This StateManager is only used for Twitter Aurora environment.
 * It extends CuratorStateManager but adding more features, given the Twitter environment:
 * <p>
 * 1. Tunneling if needed
 * 2. Will not setSchedulerLocation since we would not start the Scheduler
 * 2. getSchedulerLocation return NO SCHEDULER_REST_ENDPOINT directly
 */
public class AuroraStateManager extends CuratorStateManager {
    private static final Logger LOG = Logger.getLogger(AuroraStateManager.class.getName());
    private static final String NO_SCHEDULER_REST_ENDPOINT = "no_scheduler_endpoint";
    private List<Process> processHandles = new ArrayList<>();

    @Override
    public void initialize(Map<Object, Object> conf) {
        Map<Object, Object> newConf = new HashMap<>();
        newConf.putAll(conf);

        String zkHost = (String) conf.get(Constants.ZKHOST);
        Integer zkPort = Integer.parseInt((String) conf.get(Constants.ZKPORT));
        String zkRoot = (String) conf.get(Constants.ZKROOT);
        String tunnelHost = (String) conf.get(Constants.TUNNEL);

        // Check tunnel needed
        int timeout = Integer.parseInt((String) conf.get(Constants.ZK_CONNECTION_TIMEOUT_MS));
        boolean isVerbose = Boolean.parseBoolean((String) conf.get(Constants.HERON_VERBOSE));

        if (!NetworkUtility.isLocationReachable(timeout, 1, 1000, zkHost, zkPort, isVerbose)) {
            LOG.info("Will use tunnelling " + zkHost + ":" + zkPort);
            int freePort = NetworkUtility.getFreePort();
            if (isVerbose) {
                LOG.info("Opening up tunnel to " + zkHost + " at " + freePort);
            }
            Process zkTunnel = ShellUtility.setupTunnel(
                    isVerbose, tunnelHost, freePort, zkHost, zkPort);
            processHandles.add(zkTunnel);

            // Tunnel sometime takes longer to setup.
            if (NetworkUtility.isLocationReachable(timeout, 10, 1000, "localhost", freePort, isVerbose)) {
                String newHostPort = String.format("%s:%d", "localhost", freePort);
                LOG.info("Connecting to zookeeper at " + newHostPort);
                newConf.put(Constants.ZK_CONNECTION_STRING, newHostPort);
                newConf.put(ROOT_ADDRESS, zkRoot);
            } else {
                throw new RuntimeException("Failed to setup the tunnel to Zookeeper Server");
            }
        } else {
            String newHostPort = String.format("%s:%d", zkHost, zkPort);
            LOG.info("Connecting to zookeeper at " + String.format("%s:%d", zkHost, zkPort));
            newConf.put(Constants.ZK_CONNECTION_STRING, newHostPort);
            newConf.put(ROOT_ADDRESS, zkRoot);
        }

        super.initialize(newConf);
    }

    @Override
    public ListenableFuture<Boolean> setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName) {
        LOG.info("No need to set SchedulerLocation: " + topologyName);
        SettableFuture<Boolean> future = SettableFuture.create();
        future.set(true);
        return future;
    }

    @Override
    public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(WatchCallback watcher, String topologyName) {
        LOG.info("Topology does not have a scheduler: " + topologyName);
        SettableFuture<Scheduler.SchedulerLocation> future = SettableFuture.create();
        future.set(Scheduler.SchedulerLocation.newBuilder()
                .setHttpEndpoint(NO_SCHEDULER_REST_ENDPOINT)
                .setTopologyName(topologyName)
                .build());
        return future;
    }

    @Override
    public void close() {
        for (Process p : processHandles) {
            p.destroy();
        }
        processHandles.clear();

        super.close();
    }
}
