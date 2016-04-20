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

package com.twitter.heron.spi.scheduler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;

public class NullScheduler implements IScheduler {

    @Override
    public void initialize(Config config, Config runtime) {

    }

    @Override
    public void close() {

    }

    @Override
    public void schedule(PackingPlan packing) {

    }

    @Override
    public boolean onKill(Scheduler.KillTopologyRequest request) {
        return true;
    }

    @Override
    public boolean onActivate(Scheduler.ActivateTopologyRequest request) {
        return true;
    }

    @Override
    public boolean onDeactivate(Scheduler.DeactivateTopologyRequest request) {
        return true;
    }

    @Override
    public boolean onRestart(Scheduler.RestartTopologyRequest request) {
        return true;
    }
}

