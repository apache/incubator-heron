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


package org.apache.heron.streamlet.impl.streamlets;

import java.util.Set;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.SerializableSupplier;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.sources.SupplierSource;

/**
 * SupplierStreamlet is a very quick and flexible way of creating a Streamlet
 * from an user supplied Supplier Function. The supplier function is the
 * source of all tuples for this Streamlet.
 */
public class SupplierStreamlet<R> extends StreamletImpl<R> {
  private SerializableSupplier<R> supplier;

  public SupplierStreamlet(SerializableSupplier<R> supplier) {
    this.supplier = supplier;
    setNumPartitions(1);
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.SUPPLIER, stageNames);
    bldr.setSpout(getName(), new SupplierSource<R>(supplier), getNumPartitions());
    return true;
  }
}
