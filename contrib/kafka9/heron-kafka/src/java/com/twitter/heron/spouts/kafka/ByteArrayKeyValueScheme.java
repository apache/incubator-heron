package com.twitter.heron.spouts.kafka;

import com.google.common.collect.ImmutableMap;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

import java.util.List;

public class ByteArrayKeyValueScheme implements KeyValueScheme {

    @Override
    public List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
        if (key == null) {
            return deserialize(value);
        }
        return new Values(ImmutableMap.of(key, value));
    }

    @Override
    public List<Object> deserialize(byte[] byteBuffer) {
        return new Values(byteBuffer);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("bytes");
    }
}
