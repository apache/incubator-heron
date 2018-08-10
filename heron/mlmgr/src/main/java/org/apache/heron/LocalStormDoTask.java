package org.apache.samoa;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.samoa.topology.impl.StormSamoaUtils;
import org.apache.samoa.topology.impl.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.configuration.Configuration;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

/**
 * The main class to execute a SAMOA task in LOCAL mode in Storm.
 * 
 * @author Arinto Murdopo
 * 
 */
public class LocalStormDoTask {

  private static final Logger logger = LoggerFactory.getLogger(LocalStormDoTask.class);
  private static final String EXECUTION_DURATION_KEY ="samoa.storm.local.mode.execution.duration";
  private static final String SAMOA_STORM_PROPERTY_FILE_LOC ="samoa-storm.properties";
  /**
   * The main method.
   * 
   * @param args
   *          the arguments
   */
  public static void main(String[] args) {

    List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

    int numWorker = StormSamoaUtils.numWorkers(tmpArgs);

    args = tmpArgs.toArray(new String[0]);

    // convert the arguments into Storm topology
    StormTopology stormTopo = StormSamoaUtils.argsToTopology(args);
    String topologyName = stormTopo.getTopologyName();

    Config conf = new Config();
    // conf.putAll(Utils.readStormConfig());
    conf.setDebug(false);

    // local mode
    conf.setMaxTaskParallelism(numWorker);

    backtype.storm.LocalCluster cluster = new backtype.storm.LocalCluster();
    cluster.submitTopology(topologyName, conf, stormTopo.getStormBuilder().createTopology());

    // Read local mode execution duration from property file
    Configuration stormConfig = StormSamoaUtils.getPropertyConfig(LocalStormDoTask.SAMOA_STORM_PROPERTY_FILE_LOC);
    long executionDuration= stormConfig.getLong(LocalStormDoTask.EXECUTION_DURATION_KEY);
    backtype.storm.utils.Utils.sleep(executionDuration * 1000);

    cluster.killTopology(topologyName);
    cluster.shutdown();

  }
}
