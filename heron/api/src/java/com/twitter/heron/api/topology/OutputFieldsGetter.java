package com.twitter.heron.api.topology;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.utils.Utils;
import java.util.HashMap;
import java.util.Map;

public class OutputFieldsGetter implements OutputFieldsDeclarer {
    private Map<String, TopologyAPI.StreamSchema.Builder> _fields = 
                   new HashMap<String, TopologyAPI.StreamSchema.Builder>();

    public void declare(Fields fields) {
        declare(false, fields);
    }

    public void declare(boolean direct, Fields fields) {
        declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
    }

    public void declareStream(String streamId, Fields fields) {
        declareStream(streamId, false, fields);
    }

    public void declareStream(String streamId, boolean direct, Fields fields) {
        if(_fields.containsKey(streamId)) {
            throw new IllegalArgumentException("Fields for " + streamId + " already set");
        }
        TopologyAPI.StreamSchema.Builder bldr = TopologyAPI.StreamSchema.newBuilder();
        for (int i = 0; i < fields.size(); ++i) {
          TopologyAPI.StreamSchema.KeyType.Builder ktBldr = TopologyAPI.StreamSchema.KeyType.newBuilder();
          ktBldr.setKey(fields.get(i));
          ktBldr.setType(TopologyAPI.Type.OBJECT);
          bldr.addKeys(ktBldr);
        }
        _fields.put(streamId, bldr);
    }


    public Map<String, TopologyAPI.StreamSchema.Builder> getFieldsDeclaration() {
        return _fields;
    }
}
