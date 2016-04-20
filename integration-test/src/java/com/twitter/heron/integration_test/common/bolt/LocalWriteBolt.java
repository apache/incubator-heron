package com.twitter.heron.integration_test.common.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

/**
 * Given a local file path, this bolt will emit every line of received in String format, as well as write
 * every line to the local file.
 * <p>
 * Note: The number of parallelisms for this spout should be equal to the number of files/paths
 * to read.
 */


public class LocalWriteBolt extends BaseBasicBolt {
  private String path;
  private BufferedWriter bw = null;

  public LocalWriteBolt(String path) {
    this.path = path;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext) {
    try {
      File outputFile = new File(path);
      if (!outputFile.exists()) {
        outputFile.createNewFile();
      }
      bw = new BufferedWriter(
          new FileWriter(outputFile.getAbsoluteFile(), true),
          1024 * 1024
      );
    } catch (IOException e) {
      // Clean stuff if any exceptions
      try {
        if (bw != null)
          bw.close();
      } catch (Exception e1) {
        throw new RuntimeException("Unable to close file writer", e1);
      }
      throw new RuntimeException("Failed to create BufferedWriter from file path", e);
    }
  }

  // We do not explicitly close the buffered writer in LocalWriteBolt as we cannot guarantee which tuple is the last tuple
// Writer will be closed automatically on process close
  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      String data = input.getString(0);
      bw.write(data);
      bw.newLine();
      bw.flush();
    } catch (Exception e) {
      // Clean stuff if any exceptions
      try {
        // Close the outmost is enough
        if (bw != null) {
          bw.close();
        }
      } catch (Exception e1) {
        throw new RuntimeException("Unable to close stream writer", e1);
      }
      throw new RuntimeException("Unable to write to file or emit tuples", e);
    }
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("string"));
  }
}
