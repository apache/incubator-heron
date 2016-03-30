package org.apache.storm;

import java.util.Map;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.ConfigUtils;

import com.twitter.heron.api.HeronSubmitter;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should run your program
 * with the "storm jar" command from the command-line, and then use this class to
 * submit your topologies.
 */
public class StormSubmitter {

  /**
   * Submits a topology to run on the cluster. A topology runs forever or until
   * explicitly killed.
   *
   *
   * @param name the name of the storm.
   * @param stormConfig the topology-specific configuration. See {@link Config}.
   * @param topology the processing to execute.
   * @throws AlreadyAliveException if a topology with this name is already running
   * @throws InvalidTopologyException if an invalid topology was submitted
   */
  public static void submitTopology(String name, Map stormConfig, StormTopology topology)
          throws AlreadyAliveException, InvalidTopologyException {

    // First do config translation
    com.twitter.heron.api.Config heronConfig = ConfigUtils.translateConfig(stormConfig);

    // Now submit a heron topology
    try {
      HeronSubmitter.submitTopology(name, heronConfig, topology.getStormTopology());
    } catch (com.twitter.heron.api.exception.AlreadyAliveException e) {
      throw new AlreadyAliveException();
    } catch (com.twitter.heron.api.exception.InvalidTopologyException e) {
      throw new InvalidTopologyException();
    }
  }
}
