package org.apache.samoa.learners.clusterers;

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
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.instances.Instance;

import net.jcip.annotations.Immutable;

/**
 * The Class ClusteringContentEvent.
 */
@Immutable
final public class ClusteringContentEvent implements ContentEvent {

  private static final long serialVersionUID = -7746983521296618922L;
  private Instance instance;
  private boolean isLast = false;
  private String key;
  private boolean isSample;

  public ClusteringContentEvent() {
    // Necessary for kryo serializer
  }

  /**
   * Instantiates a new clustering event.
   * 
   * @param index
   *          the index
   * @param instance
   *          the instance
   */
  public ClusteringContentEvent(long index, Instance instance) {
    /*
     * if (instance != null) { this.instance = new
     * SerializableInstance(instance); }
     */
    this.instance = instance;
    this.setKey(Long.toString(index));
  }

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public void setKey(String str) {
    this.key = str;
  }

  @Override
  public boolean isLastEvent() {
    return this.isLast;
  }

  public void setLast(boolean isLast) {
    this.isLast = isLast;
  }

  public Instance getInstance() {
    return this.instance;
  }

  public boolean isSample() {
    return isSample;
  }

  public void setSample(boolean b) {
    this.isSample = b;
  }
}
