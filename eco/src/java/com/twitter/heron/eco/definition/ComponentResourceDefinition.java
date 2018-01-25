//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.eco.definition;

public class ComponentResourceDefinition {

  private String id;

  private String ram;

  private double cpu;

  private String disk;


  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getRam() {
    return ram;
  }

  public void setRam(String ram) {
    this.ram = ram;
  }

  public double getCpu() {
    return cpu;
  }

  public void setCpu(double cpu) {
    this.cpu = cpu;
  }

  public String getDisk() {
    return disk;
  }

  public void setDisk(String disk) {
    this.disk = disk;
  }
}
