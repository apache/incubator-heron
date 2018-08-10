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
/**
 * License
 */
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.ClusteringEvaluationContentEvent;
import org.apache.samoa.evaluation.ClusteringResultContentEvent;
import org.apache.samoa.instances.DenseInstance;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import weka.core.Instance;

/**
 * The Class LearnerProcessor.
 */
final public class LocalClustererProcessor implements Processor {

  /**
     *
     */
  private static final long serialVersionUID = -1577910988699148691L;
  private static final Logger logger = LoggerFactory
      .getLogger(LocalClustererProcessor.class);
  private LocalClustererAdapter model;
  private Stream outputStream;
  private int modelId;
  private long instancesCount = 0;
  private long sampleFrequency = 1000;

  public long getSampleFrequency() {
    return sampleFrequency;
  }

  public void setSampleFrequency(long sampleFrequency) {
    this.sampleFrequency = sampleFrequency;
  }

  /**
   * Sets the learner.
   * 
   * @param model
   *          the model to set
   */
  public void setLearner(LocalClustererAdapter model) {
    this.model = model;
  }

  /**
   * Gets the learner.
   * 
   * @return the model
   */
  public LocalClustererAdapter getLearner() {
    return model;
  }

  /**
   * Set the output streams.
   * 
   * @param outputStream
   *          the new output stream {@link PredictionCombinerPE}.
   */
  public void setOutputStream(Stream outputStream) {

    this.outputStream = outputStream;
  }

  /**
   * Gets the output stream.
   * 
   * @return the output stream
   */
  public Stream getOutputStream() {
    return outputStream;
  }

  /**
   * Gets the instances count.
   * 
   * @return number of observation vectors used in training iteration.
   */
  public long getInstancesCount() {
    return instancesCount;
  }

  /**
   * Update stats.
   * 
   * @param event
   *          the event
   */
  private void updateStats(ContentEvent event) {
    Instance instance;
    if (event instanceof ClusteringContentEvent) {
      // Local Clustering
      ClusteringContentEvent ev = (ClusteringContentEvent) event;
      instance = ev.getInstance();
      DataPoint point = new DataPoint(instance, Integer.parseInt(event.getKey()));
      model.trainOnInstance(point);
      instancesCount++;
    }

    if (event instanceof ClusteringResultContentEvent) {
      // Global Clustering
      ClusteringResultContentEvent ev = (ClusteringResultContentEvent) event;
      Clustering clustering = ev.getClustering();

      for (int i = 0; i < clustering.size(); i++) {
        instance = new DenseInstance(1.0, clustering.get(i).getCenter());
        instance.setDataset(model.getDataset());
        DataPoint point = new DataPoint(instance, Integer.parseInt(event.getKey()));
        model.trainOnInstance(point);
        instancesCount++;
      }
    }

    if (instancesCount % this.sampleFrequency == 0) {
      logger.info("Trained model using {} events with classifier id {}", instancesCount, this.modelId); // getId());
    }
  }

  /**
   * On event.
   * 
   * @param event
   *          the event
   * @return true, if successful
   */
  @Override
  public boolean process(ContentEvent event) {

    if (event.isLastEvent() ||
        (instancesCount > 0 && instancesCount % this.sampleFrequency == 0)) {
      if (model.implementsMicroClusterer()) {

        Clustering clustering = model.getMicroClusteringResult();

        ClusteringResultContentEvent resultEvent = new ClusteringResultContentEvent(clustering, event.isLastEvent());

        this.outputStream.put(resultEvent);
      }
    }

    updateStats(event);
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.Processor#onCreate(int)
   */
  @Override
  public void onCreate(int id) {
    this.modelId = id;
    model = model.create();
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.Processor#newProcessor(samoa.core.Processor)
   */
  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    LocalClustererProcessor newProcessor = new LocalClustererProcessor();
    LocalClustererProcessor originProcessor = (LocalClustererProcessor) sourceProcessor;
    if (originProcessor.getLearner() != null) {
      newProcessor.setLearner(originProcessor.getLearner().create());
    }
    newProcessor.setOutputStream(originProcessor.getOutputStream());
    return newProcessor;
  }
}
