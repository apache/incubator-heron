package com.twitter.heron.integration_test.common.spout;

import java.io.BufferedReader;
import java.util.Map;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.integration_test.common.HdfsHelper;

/**
 * Given a list of hdfs path, the spout will emit every line of of the file in String format.
 * When we fetch all items from hdfs BufferedReader, we would set the BufferedReader as null. We
 * would like to check whether (BufferedReader == null) to see whether the data is taken completely.
 * <p>
 * Note1: The hadoop cluster config should be provided through the classpath
 * Note2: The number of parallelisms for this spout should be equal to the number of files/paths
 * to read.
 */
public class HdfsStringSpout extends BaseRichSpout {
  private static final long serialVersionUID = 5452173269208083710L;
  // Hadoop file related
  private BufferedReader br = null;
  // Topology related
  private SpoutOutputCollector collector;
  private String[] paths;

  public HdfsStringSpout(String path) {
    this(new String[]{path});
  }

  public HdfsStringSpout(String[] paths) {
    this.paths = paths;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("string"));
  }

  @Override
  public void open(Map<String, Object> stormConf,
                   TopologyContext context,
                   SpoutOutputCollector collector) {
    int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
    // Pre-condition: the number of tasks is equal to the number of files to read
    if (paths.length != numTasks) {
      throw new RuntimeException(
          String.format("Number of hdfs files %d not equal to number of tasks %d",
              paths.length, numTasks));
    }

    this.collector = collector;

    try {

      int index = context.getThisTaskIndex();
      String path = paths[index];

      // read file from hdfs
      br = new BufferedReader(
          HdfsHelper.getHdfsStreamReader(path),
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
        throw new RuntimeException("Unable to close stream reader", e1);
      }

      // Here we should not close the FileSystem explicitly
      // Since different threads in a process may use a shared object in HDFS,
      // if we close the FileSystem here, it will close the shared object, and other threads
      // reading data by using this shared object will throw Exception"
      // The FileSystem will be closed automatically when the process dies (the topology is killed)

      throw new RuntimeException("Failed to access the HDFS", e);
    }
  }

  @Override
  public void nextTuple() {
    if (br == null) {
      return;
    }

    try {
      String line = "";
      if ((line = br.readLine()) != null) {
        collector.emit(new Values(line));
      } else {
        br.close();
        br = null;
      }

    } catch (Exception e) {
      // Clean stuff if any exceptions
      try {
        // Close the outmost is enough
        br.close();
      } catch (Exception e1) {
        throw new RuntimeException("Unable to close stream reader", e1);
      }
      throw new RuntimeException("Unable to emit tuples normally", e);
    }
  }
}