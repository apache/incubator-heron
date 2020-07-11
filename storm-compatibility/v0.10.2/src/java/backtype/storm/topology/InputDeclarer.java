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

package backtype.storm.topology;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;

//import backtype.storm.generated.Grouping;

@SuppressWarnings("rawtypes")
public interface InputDeclarer<T extends InputDeclarer> {
  T fieldsGrouping(String componentId, Fields fields);

  T fieldsGrouping(String componentId, String streamId, Fields fields);

  T globalGrouping(String componentId);

  T globalGrouping(String componentId, String streamId);

  T shuffleGrouping(String componentId);

  T shuffleGrouping(String componentId, String streamId);

  T localOrShuffleGrouping(String componentId);

  T localOrShuffleGrouping(String componentId, String streamId);

  T noneGrouping(String componentId);

  T noneGrouping(String componentId, String streamId);

  T allGrouping(String componentId);

  T allGrouping(String componentId, String streamId);

  T directGrouping(String componentId);

  T directGrouping(String componentId, String streamId);

  T customGrouping(String componentId, CustomStreamGrouping grouping);

  T customGrouping(String componentId, String streamId, CustomStreamGrouping grouping);

  //  T grouping(GlobalStreamId id, Grouping grouping);
}
