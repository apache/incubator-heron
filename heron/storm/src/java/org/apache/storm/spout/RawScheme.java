package org.apache.storm.spout;

import java.util.List;

import org.apache.storm.tuple.Fields;

import static org.apache.storm.utils.Utils.tuple;

public class RawScheme implements Scheme {
    public List<Object> deserialize(byte[] ser) {
        return tuple(ser);
    }

    public Fields getOutputFields() {
        return new Fields("bytes");
    }
}
