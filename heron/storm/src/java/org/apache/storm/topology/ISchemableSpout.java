package org.apache.storm.topology;

import org.apache.storm.spout.Scheme;

public interface ISchemableSpout {
     Scheme getScheme();
     void setScheme(Scheme scheme);
}
