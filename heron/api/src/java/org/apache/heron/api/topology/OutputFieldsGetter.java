/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.api.topology;

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.utils.Utils;

public class OutputFieldsGetter implements OutputFieldsDeclarer {
  private Map<String, TopologyAPI.StreamSchema.Builder> fields =
      new HashMap<String, TopologyAPI.StreamSchema.Builder>();

  public void declare(Fields pFields) {
    declare(false, pFields);
  }

  public void declare(boolean direct, Fields pFields) {
    declareStream(Utils.DEFAULT_STREAM_ID, direct, pFields);
  }

  public void declareStream(String streamId, Fields pFields) {
    declareStream(streamId, false, pFields);
  }

  public void declareStream(String streamId, boolean direct, Fields pFields) {
    if (fields.containsKey(streamId)) {
      throw new IllegalArgumentException("Fields for " + streamId + " already set");
    }
    TopologyAPI.StreamSchema.Builder bldr = TopologyAPI.StreamSchema.newBuilder();
    for (int i = 0; i < pFields.size(); ++i) {
      TopologyAPI.StreamSchema.KeyType.Builder ktBldr =
          TopologyAPI.StreamSchema.KeyType.newBuilder();
      ktBldr.setKey(pFields.get(i));
      ktBldr.setType(TopologyAPI.Type.OBJECT);
      bldr.addKeys(ktBldr);
    }
    fields.put(streamId, bldr);
  }


  public Map<String, TopologyAPI.StreamSchema.Builder> getFieldsDeclaration() {
    return fields;
  }
}
