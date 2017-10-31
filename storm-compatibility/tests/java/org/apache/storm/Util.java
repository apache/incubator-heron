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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Tuple;

public final class Util {

  private static final long MILLIS_IN_SEC = 1000;
  private static final String SYSTEM_COMPONENT_ID = "__system";
  private static final String SYSTEM_TICK_STREAM_ID = "__tick";

  private Util() {
  }

  public static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(SYSTEM_COMPONENT_ID)
        && tuple.getSourceStreamId().equals(SYSTEM_TICK_STREAM_ID);
  }

  public static void runTopologyLocally(StormTopology topology,
                                        String topologyName,
                                        Config conf,
                                        int runtimeInSeconds)
      throws InterruptedException, AlreadyAliveException, InvalidTopologyException,
        NotAliveException {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);
    Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }
}
