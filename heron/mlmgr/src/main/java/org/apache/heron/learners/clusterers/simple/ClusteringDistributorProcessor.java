package org.apache.samoa.learners.clusterers.simple;

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
import org.apache.samoa.learners.clusterers.ClusteringContentEvent;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.topology.Stream;

/**
 * The Class ClusteringDistributorPE.
 */
public class ClusteringDistributorProcessor implements Processor {

  private static final long serialVersionUID = -1550901409625192730L;

  private Stream outputStream;
  private Stream evaluationStream;
  private int numInstances;

  public Stream getOutputStream() {
    return outputStream;
  }

  public void setOutputStream(Stream outputStream) {
    this.outputStream = outputStream;
  }

  public Stream getEvaluationStream() {
    return evaluationStream;
  }

  public void setEvaluationStream(Stream evaluationStream) {
    this.evaluationStream = evaluationStream;
  }

  /**
   * Process event.
   * 
   * @param event
   *          the event
   * @return true, if successful
   */
  public boolean process(ContentEvent event) {
    // distinguish between ClusteringContentEvent and
    // ClusteringEvaluationContentEvent
    if (event instanceof ClusteringContentEvent) {
      ClusteringContentEvent cce = (ClusteringContentEvent) event;
      outputStream.put(event);
      if (cce.isSample()) {
        evaluationStream.put(new ClusteringEvaluationContentEvent(null,
            new DataPoint(cce.getInstance(), numInstances++), cce.isLastEvent()));
      }
    } else if (event instanceof ClusteringEvaluationContentEvent) {
      evaluationStream.put(event);
    }
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.Processor#newProcessor(samoa.core.Processor)
   */
  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    ClusteringDistributorProcessor newProcessor = new ClusteringDistributorProcessor();
    ClusteringDistributorProcessor originProcessor = (ClusteringDistributorProcessor) sourceProcessor;
    if (originProcessor.getOutputStream() != null)
      newProcessor.setOutputStream(originProcessor.getOutputStream());
    if (originProcessor.getEvaluationStream() != null)
      newProcessor.setEvaluationStream(originProcessor.getEvaluationStream());
    return newProcessor;
  }

  public void onCreate(int id) {
  }
}
