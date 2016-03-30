package org.apache.storm;

import java.util.Map;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;

// import org.apache.storm.generated.ClusterSummary;
// import org.apache.storm.generated.KillOptions;
// import org.apache.storm.generated.SubmitOptions;
// import org.apache.storm.generated.RebalanceOptions;
// import org.apache.storm.generated.TopologyInfo;


public interface ILocalCluster {
    void submitTopology(String topologyName, Map conf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException;
    // void submitTopologyWithOpts(String topologyName, Map conf, StormTopology topology, SubmitOptions submitOpts) throws AlreadyAliveException, InvalidTopologyException;
    void killTopology(String topologyName) throws NotAliveException;
    // void killTopologyWithOpts(String name, KillOptions options) throws NotAliveException;
    void activate(String topologyName) throws NotAliveException;
    void deactivate(String topologyName) throws NotAliveException;
    // void rebalance(String name, RebalanceOptions options) throws NotAliveException;
    void shutdown();
    String getTopologyConf(String id);
    StormTopology getTopology(String id);
    // ClusterSummary getClusterInfo();
    // TopologyInfo getTopologyInfo(String id);
    Map getState();
}
