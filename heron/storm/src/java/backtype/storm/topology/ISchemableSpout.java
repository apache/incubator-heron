package backtype.storm.topology;

import backtype.storm.spout.Scheme;

public interface ISchemableSpout {
     Scheme getScheme();
     void setScheme(Scheme scheme);
}
