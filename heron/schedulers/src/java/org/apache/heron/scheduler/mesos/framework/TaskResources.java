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

package org.apache.heron.scheduler.mesos.framework;

import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.Protos;

/**
 * TaskResources is an utils class for easy access of Protos.Offer
 */

public class TaskResources {
  public static final String CPUS_RESOURCE_NAME = "cpus";
  public static final String MEM_RESOURCE_NAME = "mem";
  public static final String DISK_RESOURCE_NAME = "disk";
  public static final String PORT_RESOURCE_NAME = "ports";

  public static class Range {
    public long rangeStart;
    public long rangeEnd;

    public Range(long rangeStart, long rangeEnd) {
      this.rangeStart = rangeStart;
      this.rangeEnd = rangeEnd;
    }
  }

  private double cpu;
  private double mem;
  private double disk;
  // # of ports request
  private int ports;

  private List<Range> portsHold = new ArrayList<>();

  public TaskResources(double cpu, double mem, double disk, List<Range> portsResources) {
    this.cpu = cpu;
    this.mem = mem;
    this.disk = disk;
    this.portsHold = portsResources;
    for (Range r : portsResources) {
      this.ports += r.rangeEnd - r.rangeStart + 1;
    }
  }

  public TaskResources(double cpu, double mem, double disk, int ports) {
    this.cpu = cpu;
    this.mem = mem;
    this.disk = disk;
    this.ports = ports;
    // No specific ports value needed. So portsHold is empty
  }

  // Whether this resource can satisfy the TaskResources needed from parameter
  public boolean canSatisfy(TaskResources needed) {
    return this.ports >= needed.ports
        && (this.cpu >= needed.cpu)
        && (this.mem >= needed.mem)
        && (this.disk >= needed.disk);
  }

  public void consume(TaskResources needed) {
    this.cpu -= needed.cpu;
    this.mem -= needed.mem;
    this.disk -= needed.disk;
    this.ports -= needed.ports;

    // Consume the port
    for (Range portRange : portsHold) {
      if (portRange.rangeEnd - portRange.rangeStart + 1 > needed.ports) {
        long rangeEnd = portRange.rangeStart + needed.ports - 1;

        needed.portsHold.add(new Range(portRange.rangeStart, rangeEnd));
        // And then consume us by update the range value
        portRange.rangeStart = portRange.rangeStart + needed.ports;

        // Consumptino done. Break the loop
        break;
      }
    }
  }

  public List<Range> getPortsHold() {
    return portsHold;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMem() {
    return mem;
  }

  public double getDisk() {
    return disk;
  }

  public int getPorts() {
    return ports;
  }

  public String toString() {
    return String.format("cpu: %f; mem: %f; disk: %f; ports: %d.",
        this.cpu, this.mem, this.disk, this.ports);
  }

  // A static method to construct a TaskResources from mesos Protos.Offer
  public static TaskResources apply(Protos.Offer offer, String role) {
    double cpu = 0;
    double mem = 0;
    double disk = 0;
    List<Range> portsResource = new ArrayList<>();
    for (Protos.Resource r : offer.getResourcesList()) {
      if (!r.hasRole() || r.getRole().equals("*") || r.getRole().equals(role)) {
        if (r.getName().equals(CPUS_RESOURCE_NAME)) {
          cpu = r.getScalar().getValue();
        }
        if (r.getName().equals(MEM_RESOURCE_NAME)) {
          mem = r.getScalar().getValue();
        }
        if (r.getName().equals(DISK_RESOURCE_NAME)) {
          disk = r.getScalar().getValue();
        }

        if (r.getName().equals(PORT_RESOURCE_NAME)) {
          Protos.Value.Ranges ranges = r.getRanges();
          for (Protos.Value.Range range : ranges.getRangeList()) {
            portsResource.add(new Range(range.getBegin(), range.getEnd()));
          }
        }
      }
    }

    return new TaskResources(cpu, mem, disk, portsResource);
  }
}
