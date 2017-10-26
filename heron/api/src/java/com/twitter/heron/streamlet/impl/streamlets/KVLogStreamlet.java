//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.streamlet.impl.streamlets;

import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.impl.KVStreamletImpl;
import com.twitter.heron.streamlet.impl.sinks.LogSink;

/**
 * LogStreamlet represents en empty Streamlet that is made up of elements from the parent
 * streamlet after logging each element. Since elements of the parents are just logged
 * nothing is emitted, thus this streamlet is empty.
 */
public class KVLogStreamlet<K, V> extends KVStreamletImpl<K, V> {
  private KVStreamletImpl<K, V> parent;
  private SerializableFunction<KeyValue<K, V>, String> logFormatter;

  public KVLogStreamlet(KVStreamletImpl<K, V> parent) {
    this.parent = parent;
    setNumPartitions(parent.getNumPartitions());
  }

  public KVLogStreamlet(KVStreamletImpl<K, V> parent,
      SerializableFunction<KeyValue<K, V>, String> logFormatter) {
    this.parent = parent;
    this.logFormatter = logFormatter;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    LogSink<KeyValue<K, V>> logSink;    

    if (getName() == null) {
      setName(defaultNameCalculator("kvlogger", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());

    if (null != logFormatter) {
      logSink = new LogSink<KeyValue<K, V>>(this.logFormatter);
    } else {
      logSink = new LogSink<KeyValue<K, V>>();
    }

    bldr.setBolt(getName(), logSink,
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
