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

package org.apache.heron.packing.builder;

import java.util.Comparator;

public enum SortingStrategy implements Comparator<ResourceRequirement> {
  RAM_FIRST {
    @Override
    public int compare(ResourceRequirement o1, ResourceRequirement o2) {
      int ramComparison = o1.getRamRequirement().compareTo(o2.getRamRequirement());
      return ramComparison == 0
          ? Double.compare(o1.getCpuRequirement(), o2.getCpuRequirement()) : ramComparison;
    }
  },
  CPU_FIRST {
    @Override
    public int compare(ResourceRequirement o1, ResourceRequirement o2) {
      int cpuComparison = Double.compare(o1.getCpuRequirement(), o2.getCpuRequirement());
      return cpuComparison == 0
          ? o1.getRamRequirement().compareTo(o2.getRamRequirement()) : cpuComparison;
    }
  }
}
