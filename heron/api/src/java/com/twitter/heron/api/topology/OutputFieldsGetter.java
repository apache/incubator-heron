// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.api.topology;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.utils.Utils;

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
        if (_fields.containsKey(streamId)) {
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
