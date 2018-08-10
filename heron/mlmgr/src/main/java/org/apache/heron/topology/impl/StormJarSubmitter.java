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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;

/**
 * Utility class to submit samoa-storm jar to a Storm cluster.
 * 
 * @author Arinto Murdopo
 * 
 */
public class StormJarSubmitter {

  public final static String UPLOADED_JAR_LOCATION_KEY = "UploadedJarLocation";

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {

    Config config = new Config();
    config.putAll(Utils.readCommandLineOpts());
    config.putAll(Utils.readStormConfig());

    String nimbusHost = (String) config.get(Config.NIMBUS_HOST);
    int nimbusThriftPort = Utils.getInt(config
        .get(Config.NIMBUS_THRIFT_PORT));

    System.out.println("Nimbus host " + nimbusHost);
    System.out.println("Nimbus thrift port " + nimbusThriftPort);

    System.out.println("uploading jar from " + args[0]);
    String uploadedJarLocation = StormSubmitter.submitJar(config, args[0]);

    System.out.println("Uploaded jar file location: ");
    System.out.println(uploadedJarLocation);

    Properties props = StormSamoaUtils.getProperties();
    props.setProperty(StormJarSubmitter.UPLOADED_JAR_LOCATION_KEY, uploadedJarLocation);

    File f = new File("src/main/resources/samoa-storm-cluster.properties");
    f.createNewFile();

    OutputStream out = new FileOutputStream(f);
    props.store(out, "properties file to store uploaded jar location from StormJarSubmitter");
  }
}
