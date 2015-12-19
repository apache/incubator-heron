package com.twitter.heron.api.spout;

import com.twitter.heron.api.tuple.Fields;
import java.util.List;
import static com.twitter.heron.api.utils.Utils.tuple;

public class RawScheme implements Scheme {
    public List<Object> deserialize(byte[] ser) {
        return tuple(ser);
    }

    public Fields getOutputFields() {
        return new Fields("bytes");
    }
}
