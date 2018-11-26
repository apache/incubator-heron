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
package org.apache.heron.streamlet.impl.sources;

import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.SerializableSupplier;

/**
 * SupplierSource is a way to wrap a supplier function inside a Heron Spout.
 * The SupplierSource just calls the get method of the supplied function
 * to generate the next tuple.
 */
public class SupplierSource<R> extends StreamletSource {

  private static final long serialVersionUID = 6476611751545430216L;
  private SerializableSupplier<R> supplier;

  public SupplierSource(SerializableSupplier<R> supplier) {
    this.supplier = supplier;
  }

  @Override
  public void nextTuple() {
    collector.emit(new Values(supplier.get()));
  }
}
