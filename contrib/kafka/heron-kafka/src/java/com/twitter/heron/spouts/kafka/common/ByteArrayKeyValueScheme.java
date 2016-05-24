package com.twitter.heron.spouts.kafka.common;

import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ByteArrayKeyValueScheme implements KeyValueScheme {

    @Override
    public List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
        if (key == null) {
            return deserialize(value);
        }
        Map<byte[], byte[]> kvMap = new HashMap<>();
        kvMap.put(key, value);
        return new Values(kvMap);
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
