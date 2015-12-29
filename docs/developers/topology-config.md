# Configuring Topologies

You can set topology-specific configurations using Heron's
[`Config`](http://heronproject.github.io/topology-api/com/twitter/heron/api/Config)
class and then pass an instance of `Config` to your
[`HeronSubmitter`](http://heronproject.github.io/topology-api/com/twitter/heron/api/HeronSubmitter)
when you submit your topology. Here's an example:

```java
import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;

public class MyTopology {
    public static void main(String[] args) {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setTeamEmail("stream-compute@acme.biz");
        // Apply the rest of your configuration

	// Set up your topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // Add bolts, spouts, etc.

	// Finally, submit the topology and configuration   
        HeronSubmitter.submitTopology("topology-name", conf, topologyBuilder.createTopology());
    } 
}
```

A full listing of configurable parameters can be found in the Javadoc for
Heron's
[`Config`](http://heronproject.github.io/topology-api/com/twitter/heron/api/Config)
class.

## Per-Component Configuration Overrides
