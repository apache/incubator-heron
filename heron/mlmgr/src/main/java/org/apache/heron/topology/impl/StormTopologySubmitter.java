package org.apache.samoa.topology.impl;

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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Helper class to submit SAMOA task into Storm without the need of submitting the jar file. The jar file must be
 * submitted first using StormJarSubmitter class.
 * 
 * @author Arinto Murdopo
 * 
 */
public class StormTopologySubmitter {

  public static String YJP_OPTIONS_KEY = "YjpOptions";

  private static Logger logger = LoggerFactory.getLogger(StormTopologySubmitter.class);

  public static void main(String[] args) throws IOException {
    Properties props = StormSamoaUtils.getProperties();

    String uploadedJarLocation = props.getProperty(StormJarSubmitter.UPLOADED_JAR_LOCATION_KEY);
    if (uploadedJarLocation == null) {
      logger.error("Invalid properties file. It must have key {}",
          StormJarSubmitter.UPLOADED_JAR_LOCATION_KEY);
      return;
    }

    List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));
    int numWorkers = StormSamoaUtils.numWorkers(tmpArgs);

    args = tmpArgs.toArray(new String[0]);
    StormTopology stormTopo = StormSamoaUtils.argsToTopology(args);

    Config conf = new Config();
    conf.putAll(Utils.readStormConfig());
    conf.putAll(Utils.readCommandLineOpts());
    conf.setDebug(false);
    conf.setNumWorkers(numWorkers);

    String profilerOption =
        props.getProperty(StormTopologySubmitter.YJP_OPTIONS_KEY);
    if (profilerOption != null) {
      String topoWorkerChildOpts = (String) conf.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
      StringBuilder optionBuilder = new StringBuilder();
      if (topoWorkerChildOpts != null) {
        optionBuilder.append(topoWorkerChildOpts);
        optionBuilder.append(' ');
      }
      optionBuilder.append(profilerOption);
      conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, optionBuilder.toString());
    }

    Map<String, Object> myConfigMap = new HashMap<String, Object>(conf);
    StringWriter out = new StringWriter();

    try {
      JSONValue.writeJSONString(myConfigMap, out);
    } catch (IOException e) {
      System.out.println("Error in writing JSONString");
      e.printStackTrace();
      return;
    }

    Config config = new Config();
    config.putAll(Utils.readStormConfig());

    NimbusClient nc = NimbusClient.getConfiguredClient(config);
    String topologyName = stormTopo.getTopologyName();
    try {
      System.out.println("Submitting topology with name: "
          + topologyName);
      nc.getClient().submitTopology(topologyName, uploadedJarLocation,
          out.toString(), stormTopo.getStormBuilder().createTopology());
      System.out.println(topologyName + " is successfully submitted");

    } catch (AlreadyAliveException aae) {
      System.out.println("Fail to submit " + topologyName
          + "\nError message: " + aae.get_msg());
    } catch (InvalidTopologyException ite) {
      System.out.println("Invalid topology for " + topologyName);
      ite.printStackTrace();
    } catch (TException te) {
      System.out.println("Texception for " + topologyName);
      te.printStackTrace();
    }
  }

  private static String uploadedJarLocation(List<String> tmpArgs) {
    int position = tmpArgs.size() - 1;
    String uploadedJarLocation = tmpArgs.get(position);
    tmpArgs.remove(position);
    return uploadedJarLocation;
  }
}
