/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.integration_test.common;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A helper class abstracting away the interaction with HDFS
 * Note: Adding these helper method directly in the spout code results
 * in a ClassNotFoundException for hadoop classes, while deploying a topology, even though
 * those classes are not being accessed during deploy.
 */
public final class HdfsHelper {

  private HdfsHelper() {
  }

  /**
   * Provide an InputStreamReader to read from a HDFS file
   *
   * @param path is the HDFS file location
   */
  public static InputStreamReader getHdfsStreamReader(String path) throws IOException {
    Path pt = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(conf);
    return new InputStreamReader(fileSystem.open(pt), StandardCharsets.UTF_8);
  }
}
