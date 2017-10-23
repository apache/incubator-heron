package org.apache.storm;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Tuple;

public class Util {

    private static final long MILLIS_IN_SEC = 1000;
    private static final String SYSTEM_COMPONENT_ID = "__system";
    private static final String SYSTEM_TICK_STREAM_ID = "__tick";

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(SYSTEM_TICK_STREAM_ID);
    }

    public static void runTopologyLocally(StormTopology topology,
                                           String topologyName,
                                           Config conf,
                                           int runtimeInSeconds)
            throws InterruptedException, AlreadyAliveException, InvalidTopologyException, NotAliveException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
}
