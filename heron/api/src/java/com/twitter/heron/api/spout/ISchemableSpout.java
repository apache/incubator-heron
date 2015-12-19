package com.twitter.heron.api.spout;


public interface ISchemableSpout {
     Scheme getScheme();
     void setScheme(Scheme scheme);
}
