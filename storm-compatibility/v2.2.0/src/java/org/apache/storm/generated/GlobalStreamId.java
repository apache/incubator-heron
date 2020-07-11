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

package org.apache.storm.generated;

public class GlobalStreamId {
  private String componentId; // required
  private String streamId; // required

  public GlobalStreamId() {
  }

  public GlobalStreamId(
      String componentId,
      String streamId) {
    this.componentId = componentId;
    this.streamId = streamId;
  }

  public void clear() {
    this.componentId = null;
    this.streamId = null;
  }

  public String get_componentId() {
    return this.componentId;
  }

  public void set_componentId(String newComponentId) {
    this.componentId = newComponentId;
  }

  public void unset_componentId() {
    this.componentId = null;
  }

  public String get_streamId() {
    return this.streamId;
  }

  public void set_streamId(String newStreamId) {
    this.streamId = newStreamId;
  }

  public void unset_streamId() {
    this.streamId = null;
  }
}
