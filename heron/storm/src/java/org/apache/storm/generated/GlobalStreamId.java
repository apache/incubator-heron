/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.generated;

import java.io.Serializable;

public class GlobalStreamId implements Serializable {
  private static final long serialVersionUID = 1873909238460677921L;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GlobalStreamId that = (GlobalStreamId) o;

    if (componentId != null ? !componentId.equals(that.componentId) : that.componentId != null)
      return false;
    return streamId != null ? streamId.equals(that.streamId) : that.streamId == null;
  }

  @Override
  public int hashCode() {
    int result = componentId != null ? componentId.hashCode() : 0;
    result = 31 * result + (streamId != null ? streamId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "GlobalStreamId{componentId='" + componentId + "', streamId='" + streamId + "'}";
  }
}
