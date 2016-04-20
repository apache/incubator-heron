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

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.utils.Runtime;

/**
 * Handles runtime tasks like kill/restart/activate/deactivate for
 * heron topology launched in the Aurora scheduler.
 */

public class AuroraRuntimeManager implements IRuntimeManager {
    private Config config;
    private Config runtime;

    @Override
    public void initialize(Config config, Config runtime) {
        this.config = config;
        this.runtime = runtime;
    }

    @Override
    public void close() {
        // Nothing to do here
    }

    @Override
    public boolean prepareRestart(Integer containerId) {
        return true;
    }

    @Override
    public boolean postRestart(Integer containerId) {
        String topologyName = Runtime.topologyName(runtime);
        return AuroraUtils.restartAuroraJob(
                topologyName,
                Context.cluster(config),
                Context.role(config),
                Context.environ(config),
                containerId,
                true);
    }

    @Override
    public boolean prepareDeactivate() {
        return true;
    }

    @Override
    public boolean postDeactivate() {
        return true;
    }

    @Override
    public boolean prepareActivate() {
        return true;
    }

    @Override
    public boolean postActivate() {
        return true;
    }

    @Override
    public boolean prepareKill() {
        return true;
    }

    @Override
    public boolean postKill() {
        String topologyName = Runtime.topologyName(runtime);
        return AuroraUtils.killAuroraJob(
                topologyName,
                Context.cluster(config),
                Context.role(config),
                Context.environ(config),
                true);
    }
}
