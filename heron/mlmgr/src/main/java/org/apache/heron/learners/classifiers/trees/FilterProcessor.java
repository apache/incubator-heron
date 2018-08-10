package org.apache.samoa.learners.classifiers.trees;

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
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.InstancesContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Filter Processor that stores and filters the instances before sending them to the Model Aggregator Processor.
 * 
 * @author Arinto Murdopo
 * 
 */
final class FilterProcessor implements Processor {

  private static final long serialVersionUID = -1685875718300564885L;
  private static final Logger logger = LoggerFactory.getLogger(FilterProcessor.class);

  private int processorId;

  private final Instances dataset;
  private InstancesHeader modelContext;

  // available streams
  private Stream outputStream;

  // private constructor based on Builder pattern
  private FilterProcessor(Builder builder) {
    this.dataset = builder.dataset;
    this.batchSize = builder.batchSize;
    this.delay = builder.delay;
  }

  private int waitingInstances = 0;

  private int delay = 0;

  private int batchSize = 200;

  private List<InstanceContentEvent> contentEventList = new LinkedList<InstanceContentEvent>();

  @Override
  public boolean process(ContentEvent event) {
    // Receive a new instance from source
    if (event instanceof InstanceContentEvent) {
      InstanceContentEvent instanceContentEvent = (InstanceContentEvent) event;
      this.contentEventList.add(instanceContentEvent);
      this.waitingInstances++;
      if (this.waitingInstances == this.batchSize || instanceContentEvent.isLastEvent()) {
        // Send Instances
        InstancesContentEvent outputEvent = new InstancesContentEvent();
        while (!this.contentEventList.isEmpty()) {
          InstanceContentEvent ice = this.contentEventList.remove(0);
          outputEvent.add(ice.getInstanceContent());
        }
        this.waitingInstances = 0;
        this.outputStream.put(outputEvent);
        if (this.delay > 0) {
          try {
            Thread.sleep(this.delay);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    return false;
  }

  @Override
  public void onCreate(int id) {
    this.processorId = id;
    this.waitingInstances = 0;

  }

  @Override
  public Processor newProcessor(Processor p) {
    FilterProcessor oldProcessor = (FilterProcessor) p;
    FilterProcessor newProcessor =
        new FilterProcessor.Builder(oldProcessor).build();

    newProcessor.setOutputStream(oldProcessor.outputStream);
    return newProcessor;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    return sb.toString();
  }

  void setOutputStream(Stream outputStream) {
    this.outputStream = outputStream;
  }

  /**
   * Helper method to generate new ResultContentEvent based on an instance and its prediction result.
   * 
   * @param prediction
   *          The predicted class label from the decision tree model.
   * @param inEvent
   *          The associated instance content event
   * @return ResultContentEvent to be sent into Evaluator PI or other destination PI.
   */
  private ResultContentEvent newResultContentEvent(double[] prediction, InstanceContentEvent inEvent) {
    ResultContentEvent rce = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(),
        inEvent.getClassId(), prediction, inEvent.isLastEvent());
    rce.setClassifierIndex(this.processorId);
    rce.setEvaluationIndex(inEvent.getEvaluationIndex());
    return rce;
  }

  /**
   * Builder class to replace constructors with many parameters
   * 
   * @author Arinto Murdopo
   * 
   */
  static class Builder {

    // required parameters
    private final Instances dataset;

    private int delay = 0;

    private int batchSize = 200;

    Builder(Instances dataset) {
      this.dataset = dataset;
    }

    Builder(FilterProcessor oldProcessor) {
      this.dataset = oldProcessor.dataset;
      this.delay = oldProcessor.delay;
      this.batchSize = oldProcessor.batchSize;
    }

    public Builder delay(int delay) {
      this.delay = delay;
      return this;
    }

    public Builder batchSize(int val) {
      this.batchSize = val;
      return this;
    }

    FilterProcessor build() {
      return new FilterProcessor(this);
    }
  }

}
