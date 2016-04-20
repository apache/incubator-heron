package com.twitter.heron.integration_test.common.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

/**
 * Given a list of local file paths, the spout will emit every line of the file in String format.
 * When we fetch all items from local file BufferedReader, we would set the BufferedReader as null. We
 * would like to check whether (BufferedReader == null) to see whether the data is taken completely.
 * <p>
 * Note: The number of parallelisms for this spout should be equal to the number of files/paths
 * to read.
 */
public class LocalFileSpout extends BaseRichSpout {
  // Hadoop file related
  private BufferedReader br = null;
  // Topology related
  private SpoutOutputCollector collector;
  private String[] paths;

  public LocalFileSpout(String path) {
    this(new String[]{path});
  }

  public LocalFileSpout(String[] paths) {
    this.paths = paths;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("string"));
  }

  @Override
  public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
    int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
    // Pre-condition: the number of tasks is equal to the number of files to read
    if (paths.length != numTasks) {
      throw new RuntimeException(
          String.format("Number of specified files %d not equal to number of tasks %d",
              paths.length, numTasks));
    }
    try {
      this.collector = collector;
      int index = context.getThisTaskIndex();
      String path = paths[index];
      // read from local file
      br = new BufferedReader(
          new FileReader(path),
          1024 * 1024
      );

    } catch (Exception e) {
      // Clean stuff if any exceptions
      try {
        // Close the outmost is enough
        if (br != null) {
          br.close();
        }
      } catch (Exception e1) {
        throw new RuntimeException("Unable to close file reader", e1);
      }

      // Here we should not close the FileSystem explicitly
      // Since different threads in a process may use a shared object,
      // if we close the FileSystem here, it will close the shared object, and other threads
      // reading data by using this shared object will throw Exception"
      // The FileSystem will be closed automatically when the process dies (the topology is killed)

      throw new RuntimeException("Failed to create BufferedReader from file path", e);
    }
  }

  // We do not explicitly close the buffered reader, even on EoF. This is in case more content is added
// to file, we will read that content as well
  @Override
  public void nextTuple() {
    if (br == null) {
      return;
    }

    try {
      String currentLine;
      if ((currentLine = br.readLine()) != null) {
        collector.emit(new Values(currentLine));
      } else {
        br.close();
        br = null;
      }

    } catch (Exception e) {
      // Clean stuff if any exceptions
      try {
        // Close the outmost is enough
        if (br != null) {
          br.close();
        }
      } catch (Exception e1) {
        throw new RuntimeException("Unable to close stream reader", e1);
      }
      throw new RuntimeException("Unable to emit tuples normally", e);
    }
  }
}

