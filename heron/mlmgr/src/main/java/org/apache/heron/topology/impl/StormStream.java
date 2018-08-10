package org.apache.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.UUID;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.topology.Stream;

/**
 * Abstract class to implement Storm Stream
 * 
 * @author Arinto Murdopo
 * 
 */
abstract class StormStream implements Stream, java.io.Serializable {

  /**
	 * 
	 */
  private static final long serialVersionUID = 281835563756514852L;
  protected final String outputStreamId;
  protected final InputStreamId inputStreamId;

  public StormStream(String stormComponentId) {
    this.outputStreamId = UUID.randomUUID().toString();
    this.inputStreamId = new InputStreamId(stormComponentId, this.outputStreamId);
  }

  @Override
  public abstract void put(ContentEvent contentEvent);

  String getOutputId() {
    return this.outputStreamId;
  }

  InputStreamId getInputId() {
    return this.inputStreamId;
  }

  final static class InputStreamId implements java.io.Serializable {

    /**
		 * 
		 */
    private static final long serialVersionUID = -7457995634133691295L;
    private final String componentId;
    private final String streamId;

    InputStreamId(String componentId, String streamId) {
      this.componentId = componentId;
      this.streamId = streamId;
    }

    String getComponentId() {
      return componentId;
    }

    String getStreamId() {
      return streamId;
    }
  }

  @Override
  public void setBatchSize(int batchSize) {
    // Ignore batch size
  }
}
