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


package org.apache.heron.examples.streamlet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;

/**
 * This topology demonstrates how sinks work in the Heron Streamlet API for Java.
 * In this case, the sink is a temporary file. Each value that enters the graph
 * from the source streamlet (an indefinite series of randomly generated
 * integers) is written to that temporary file.
 */
public final class FilesystemSinkTopology {
  private FilesystemSinkTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(FilesystemSinkTopology.class.getName());

  /**
   * Implements the Sink interface, which defines what happens when the toSink
   * method is invoked in a processing graph.
   */
  private static class FilesystemSink<T> implements Sink<T> {
    private static final long serialVersionUID = -96514621878356224L;
    private Path tempFilePath;
    private File tempFile;

    FilesystemSink(File f) {
      this.tempFile = f;
    }

    /**
     * The setup function is called before the sink is used. Any complex
     * instantiation logic for the sink should go here.
     */
    public void setup(Context context) {
      this.tempFilePath = Paths.get(tempFile.toURI());
    }

    /**
     * The put function defines how each incoming streamlet element is
     * actually processed. In this case, each incoming element is converted
     * to a byte array and written to the temporary file (successful writes
     * are also logged). Any exceptions are converted to RuntimeExceptions,
     * which will effectively kill the topology.
     */
    public void put(T element) {
      byte[] bytes = String.format("%s\n", element.toString()).getBytes();

      try {
        Files.write(tempFilePath, bytes, StandardOpenOption.APPEND);
        LOG.info(
            String.format("Wrote %s to %s",
                new String(bytes),
                tempFilePath.toAbsolutePath()
            )
        );
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Any cleanup logic for the sink can be applied here.
     */
    public void cleanup() {
    }
  }

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.newBuilder();

    // Creates a temporary file to write output into.
    File file = File.createTempFile("filesystem-sink-example", ".tmp");

    LOG.info(
        String.format("Ready to write to file %s",
            file.getAbsolutePath()
        )
    );

    processingGraphBuilder
        .newSource(() -> {
          // This applies a "brake" that makes the processing graph write
          // to the temporary file at a reasonable, readable pace.
          StreamletUtils.sleep(500);
          return ThreadLocalRandom.current().nextInt(100);
        })
        .setName("incoming-integers")
        // Here, the FilesystemSink implementation of the Sink
        // interface is passed to the toSink function.
        .toSink(new FilesystemSink<>(file));

    // The topology's parallelism (the number of containers across which the topology's
    // processing instance will be split) can be defined via the second command-line
    // argument (or else the default of 2 will be used).
    int topologyParallelism = StreamletUtils.getParallelism(args, 2);

    Config config = Config.newBuilder()
        .setNumContainers(topologyParallelism)
        .build();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
