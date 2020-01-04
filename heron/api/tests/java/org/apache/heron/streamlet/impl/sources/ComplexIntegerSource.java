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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Source;

public class ComplexIntegerSource implements Source<Integer> {

  private List<Integer> intList;

  ComplexIntegerSource() {
    intList = new ArrayList<>();
  }

  @Override
  public void setup(Context context) {
  }

  @Override public Collection<Integer> get() {
    intList.clear();
    int i = ThreadLocalRandom.current().nextInt(25);
    intList.add(i + 1);
    intList.add(i + 2);
    intList.add(i + 3);
    return intList;

  }

  @Override
  public void cleanup() {
  }

}
