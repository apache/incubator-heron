package backtype.storm;

import java.util.Map;

import com.twitter.heron.localmode.LocalMode;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.ConfigUtils;

public class LocalCluster implements ILocalCluster {
    private final LocalMode localMode;
    private String topologyName;
    private Map conf;
    private StormTopology topology;

    public LocalCluster() {
      this.localMode = new LocalMode();
      resetFields();
    }

    @Override
    public void submitTopology(String topologyName,
                               Map conf,
                               StormTopology topology)
        throws AlreadyAliveException, InvalidTopologyException {
      assertNotAlive();

      this.topologyName = topologyName;
      this.conf = conf;
      this.topology = topology;

      localMode.submitTopology(topologyName,
          ConfigUtils.translateConfig(conf),
          topology.getStormTopology());
    }

    @Override
    public void killTopology(String topologyName) throws NotAliveException {
      assertAlive(topologyName);
      localMode.killTopology(topologyName);
      resetFields();
    }

    @Override
    public void activate(String topologyName) throws NotAliveException {
      assertAlive(topologyName);
      localMode.activate(topologyName);
    }

    @Override
    public void deactivate(String topologyName) throws NotAliveException {
      assertAlive(topologyName);
      localMode.deactivate(topologyName);
    }

    @Override
    public void shutdown() {
      resetFields();
      localMode.shutdown();
    }

    @Override
    public String getTopologyConf(String topologyName) {
      try {
        assertAlive(topologyName);
        return this.topologyName;
      } catch (NotAliveException ex) {
        return null;
      }
    }

    @Override
    public StormTopology getTopology(String topologyName) {
      try {
        assertAlive(topologyName);
        return this.topology;
      } catch (NotAliveException ex) {
        return null;
      }
    }

    @Override
    public Map getState() {
      throw new RuntimeException("Heron does not support LocalCluster yet...");
    }

    private void resetFields() {
      this.topologyName = null;
      this.topology = null;
      this.conf = null;
    }

    private void assertAlive(String topologyName) throws NotAliveException {
      if (this.topologyName == null || !this.topologyName.equals(topologyName)) {
        throw new NotAliveException();
      }
    }

    private void assertNotAlive() throws AlreadyAliveException {
      // only one topology is allowed to run. A topology is running if the topologyName is set.
      if (this.topologyName != null) {
        throw new AlreadyAliveException();
      }
    }
}
