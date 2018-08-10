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

import static org.apache.samoa.moa.core.Utils.maxIndex;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.learners.InstanceContent;
import org.apache.samoa.learners.InstancesContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.classifiers.core.AttributeSplitSuggestion;
import org.apache.samoa.moa.classifiers.core.driftdetection.ChangeDetector;
import org.apache.samoa.moa.classifiers.core.splitcriteria.InfoGainSplitCriterion;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Model Aggegator Processor consists of the decision tree model. It connects to local-statistic PI via attribute stream
 * and control stream. Model-aggregator PI sends the split instances via attribute stream and it sends control messages
 * to ask local-statistic PI to perform computation via control stream.
 * 
 * Model-aggregator PI sends the classification result via result stream to an evaluator PI for classifier or other
 * destination PI. The calculation results from local statistic arrive to the model-aggregator PI via computation-result
 * stream.
 * 
 * @author Arinto Murdopo
 * 
 */
final class ModelAggregatorProcessor implements Processor {

  private static final long serialVersionUID = -1685875718300564886L;
  private static final Logger logger = LoggerFactory.getLogger(ModelAggregatorProcessor.class);

  private int processorId;

  private Node treeRoot;

  private int activeLeafNodeCount;
  private int inactiveLeafNodeCount;
  private int decisionNodeCount;
  private boolean growthAllowed;

  private final Instances dataset;

  // to support concurrent split
  private long splitId;
  private ConcurrentMap<Long, SplittingNodeInfo> splittingNodes;
  private BlockingQueue<Long> timedOutSplittingNodes;

  // available streams
  private Stream resultStream;
  private Stream attributeStream;
  private Stream controlStream;

  private transient ScheduledExecutorService executor;

  private final SplitCriterion splitCriterion;
  private final double splitConfidence;
  private final double tieThreshold;
  private final int gracePeriod;
  private final int parallelismHint;
  private final long timeOut;

  // private constructor based on Builder pattern
  private ModelAggregatorProcessor(Builder builder) {
    this.dataset = builder.dataset;
    this.splitCriterion = builder.splitCriterion;
    this.splitConfidence = builder.splitConfidence;
    this.tieThreshold = builder.tieThreshold;
    this.gracePeriod = builder.gracePeriod;
    this.parallelismHint = builder.parallelismHint;
    this.timeOut = builder.timeOut;
    this.changeDetector = builder.changeDetector;

    InstancesHeader ih = new InstancesHeader(dataset);
    this.setModelContext(ih);
  }

  @Override
  public boolean process(ContentEvent event) {

    // Poll the blocking queue shared between ModelAggregator and the time-out
    // threads
    Long timedOutSplitId = timedOutSplittingNodes.poll();
    if (timedOutSplitId != null) { // time out has been reached!
      SplittingNodeInfo splittingNode = splittingNodes.get(timedOutSplitId);
      if (splittingNode != null) {
        this.splittingNodes.remove(timedOutSplitId);
        this.continueAttemptToSplit(splittingNode.activeLearningNode, splittingNode.foundNode);

      }

    }

    // Receive a new instance from source
    if (event instanceof InstancesContentEvent) {
      InstancesContentEvent instancesEvent = (InstancesContentEvent) event;
      this.processInstanceContentEvent(instancesEvent);
      // Send information to local-statistic PI
      // for each of the nodes
      if (this.foundNodeSet != null) {
        for (FoundNode foundNode : this.foundNodeSet) {
          ActiveLearningNode leafNode = (ActiveLearningNode) foundNode.getNode();
          AttributeBatchContentEvent[] abce = leafNode.getAttributeBatchContentEvent();
          if (abce != null) {
            for (int i = 0; i < this.dataset.numAttributes() - 1; i++) {
              this.sendToAttributeStream(abce[i]);
            }
          }
          leafNode.setAttributeBatchContentEvent(null);
          // this.sendToControlStream(event); //split information
          // See if we can ask for splits
          if (!leafNode.isSplitting()) {
            double weightSeen = leafNode.getWeightSeen();
            // check whether it is the time for splitting
            if (weightSeen - leafNode.getWeightSeenAtLastSplitEvaluation() >= this.gracePeriod) {
              attemptToSplit(leafNode, foundNode);
            }
          }
        }
      }
      this.foundNodeSet = null;
    } else if (event instanceof LocalResultContentEvent) {
      LocalResultContentEvent lrce = (LocalResultContentEvent) event;
      Long lrceSplitId = lrce.getSplitId();
      SplittingNodeInfo splittingNodeInfo = splittingNodes.get(lrceSplitId);

      if (splittingNodeInfo != null) { // if null, that means
        // activeLearningNode has been
        // removed by timeout thread
        ActiveLearningNode activeLearningNode = splittingNodeInfo.activeLearningNode;

        activeLearningNode.addDistributedSuggestions(lrce.getBestSuggestion(), lrce.getSecondBestSuggestion());

        if (activeLearningNode.isAllSuggestionsCollected()) {
          splittingNodeInfo.scheduledFuture.cancel(false);
          this.splittingNodes.remove(lrceSplitId);
          this.continueAttemptToSplit(activeLearningNode, splittingNodeInfo.foundNode);
        }
      }
    }
    return false;
  }

  protected Set<FoundNode> foundNodeSet;

  @Override
  public void onCreate(int id) {
    this.processorId = id;

    this.activeLeafNodeCount = 0;
    this.inactiveLeafNodeCount = 0;
    this.decisionNodeCount = 0;
    this.growthAllowed = true;

    this.splittingNodes = new ConcurrentHashMap<>();
    this.timedOutSplittingNodes = new LinkedBlockingQueue<>();
    this.splitId = 0;

    // Executor for scheduling time-out threads
    this.executor = Executors.newScheduledThreadPool(8);
  }

  @Override
  public Processor newProcessor(Processor p) {
    ModelAggregatorProcessor oldProcessor = (ModelAggregatorProcessor) p;
    ModelAggregatorProcessor newProcessor = new ModelAggregatorProcessor.Builder(oldProcessor).build();

    newProcessor.setResultStream(oldProcessor.resultStream);
    newProcessor.setAttributeStream(oldProcessor.attributeStream);
    newProcessor.setControlStream(oldProcessor.controlStream);
    return newProcessor;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());

    sb.append("ActiveLeafNodeCount: ").append(activeLeafNodeCount);
    sb.append("InactiveLeafNodeCount: ").append(inactiveLeafNodeCount);
    sb.append("DecisionNodeCount: ").append(decisionNodeCount);
    sb.append("Growth allowed: ").append(growthAllowed);
    return sb.toString();
  }

  void setResultStream(Stream resultStream) {
    this.resultStream = resultStream;
  }

  void setAttributeStream(Stream attributeStream) {
    this.attributeStream = attributeStream;
  }

  void setControlStream(Stream controlStream) {
    this.controlStream = controlStream;
  }

  void sendToAttributeStream(ContentEvent event) {
    this.attributeStream.put(event);
  }

  void sendToControlStream(ContentEvent event) {
    this.controlStream.put(event);
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
  private ResultContentEvent newResultContentEvent(double[] prediction, InstanceContent inEvent) {
    ResultContentEvent rce = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(),
        inEvent.getClassId(), prediction, inEvent.isLastEvent());
    rce.setClassifierIndex(this.processorId);
    rce.setEvaluationIndex(inEvent.getEvaluationIndex());
    return rce;
  }

  private List<InstancesContentEvent> contentEventList = new LinkedList<>();

  /**
   * Helper method to process the InstanceContentEvent
   * 
   * @param instContentEvent
   */
  private void processInstanceContentEvent(InstancesContentEvent instContentEvent) {
    this.numBatches++;
    this.contentEventList.add(instContentEvent);
    if (this.numBatches == 1 || this.numBatches > 4) {
      this.processInstances(this.contentEventList.remove(0));
    }

    if (instContentEvent.isLastEvent()) {
      // drain remaining instances
      while (!contentEventList.isEmpty()) {
        processInstances(contentEventList.remove(0));
      }
    }

  }

  private int numBatches = 0;

  private void processInstances(InstancesContentEvent instContentEvent) {
    for (InstanceContent instContent : instContentEvent.getList()) {
      Instance inst = instContent.getInstance();
      boolean isTesting = instContent.isTesting();
      boolean isTraining = instContent.isTraining();
      inst.setDataset(this.dataset);
      // Check the instance whether it is used for testing or training
      // boolean testAndTrain = isTraining; //Train after testing
      double[] prediction = null;
      if (isTesting) {
        prediction = getVotesForInstance(inst, false);
        this.resultStream.put(newResultContentEvent(prediction, instContent));
      }

      if (isTraining) {
        trainOnInstanceImpl(inst);
        if (this.changeDetector != null) {
          if (prediction == null) {
            prediction = getVotesForInstance(inst);
          }
          boolean correctlyClassifies = this.correctlyClassifies(inst, prediction);
          double oldEstimation = this.changeDetector.getEstimation();
          this.changeDetector.input(correctlyClassifies ? 0 : 1);
          if (this.changeDetector.getEstimation() > oldEstimation) {
            // Start a new classifier
            logger.info("Change detected, resetting the classifier");
            this.resetLearning();
            this.changeDetector.resetLearning();
          }
        }
      }
    }
  }

  private boolean correctlyClassifies(Instance inst, double[] prediction) {
    return maxIndex(prediction) == (int) inst.classValue();
  }

  private void resetLearning() {
    this.treeRoot = null;
    // Remove nodes
    FoundNode[] learningNodes = findNodes();
    for (FoundNode learningNode : learningNodes) {
      Node node = learningNode.getNode();
      if (node instanceof SplitNode) {
        SplitNode splitNode;
        splitNode = (SplitNode) node;
        for (int i = 0; i < splitNode.numChildren(); i++) {
          splitNode.setChild(i, null);
        }
      }
    }
  }

  protected FoundNode[] findNodes() {
    List<FoundNode> foundList = new LinkedList<>();
    findNodes(this.treeRoot, null, -1, foundList);
    return foundList.toArray(new FoundNode[foundList.size()]);
  }

  protected void findNodes(Node node, SplitNode parent, int parentBranch, List<FoundNode> found) {
    if (node != null) {
      found.add(new FoundNode(node, parent, parentBranch));
      if (node instanceof SplitNode) {
        SplitNode splitNode = (SplitNode) node;
        for (int i = 0; i < splitNode.numChildren(); i++) {
          findNodes(splitNode.getChild(i), splitNode, i, found);
        }
      }
    }
  }

  /**
   * Helper method to get the prediction result. The actual prediction result is delegated to the leaf node.
   * 
   * @param inst
   * @return
   */
  private double[] getVotesForInstance(Instance inst) {
    return getVotesForInstance(inst, false);
  }

  private double[] getVotesForInstance(Instance inst, boolean isTraining) {
    double[] ret;
    FoundNode foundNode = null;
    if (this.treeRoot != null) {
      foundNode = this.treeRoot.filterInstanceToLeaf(inst, null, -1);
      Node leafNode = foundNode.getNode();
      if (leafNode == null) {
        leafNode = foundNode.getParent();
      }

      ret = leafNode.getClassVotes(inst, this);
    } else {
      int numClasses = this.dataset.numClasses();
      ret = new double[numClasses];

    }

    // Training after testing to speed up the process
    if (isTraining) {
      if (this.treeRoot == null) {
        this.treeRoot = newLearningNode(this.parallelismHint);
        this.activeLeafNodeCount = 1;
        foundNode = this.treeRoot.filterInstanceToLeaf(inst, null, -1);
      }
      trainOnInstanceImpl(foundNode, inst);
    }
    return ret;
  }

  /**
   * Helper method that represent training of an instance. Since it is decision tree, this method routes the incoming
   * instance into the correct leaf and then update the statistic on the found leaf.
   * 
   * @param inst
   */
  private void trainOnInstanceImpl(Instance inst) {
    if (this.treeRoot == null) {
      this.treeRoot = newLearningNode(this.parallelismHint);
      this.activeLeafNodeCount = 1;

    }
    FoundNode foundNode = this.treeRoot.filterInstanceToLeaf(inst, null, -1);
    trainOnInstanceImpl(foundNode, inst);
  }

  private void trainOnInstanceImpl(FoundNode foundNode, Instance inst) {

    Node leafNode = foundNode.getNode();

    if (leafNode == null) {
      leafNode = newLearningNode(this.parallelismHint);
      foundNode.getParent().setChild(foundNode.getParentBranch(), leafNode);
      activeLeafNodeCount++;
    }

    if (leafNode instanceof LearningNode) {
      LearningNode learningNode = (LearningNode) leafNode;
      learningNode.learnFromInstance(inst, this);
    }
    if (this.foundNodeSet == null) {
      this.foundNodeSet = new HashSet<>();
    }
    this.foundNodeSet.add(foundNode);
  }

  /**
   * Helper method to represent a split attempt
   * 
   * @param activeLearningNode
   *          The corresponding active learning node which will be split
   * @param foundNode
   *          The data structure to represents the filtering of the instance using the tree model.
   */
  private void attemptToSplit(ActiveLearningNode activeLearningNode, FoundNode foundNode) {
    if (!activeLearningNode.observedClassDistributionIsPure()) {
      // Increment the split ID
      this.splitId++;

      // Schedule time-out thread
      ScheduledFuture<?> timeOutHandler = this.executor.schedule(new AggregationTimeOutHandler(this.splitId,
              this.timedOutSplittingNodes), this.timeOut, TimeUnit.SECONDS);

      // Keep track of the splitting node information, so that we can continue the
      // split
      // once we receive all local statistic calculation from Local Statistic PI
      // this.splittingNodes.put(Long.valueOf(this.splitId), new
      // SplittingNodeInfo(activeLearningNode, foundNode, null));
      this.splittingNodes.put(this.splitId, new SplittingNodeInfo(activeLearningNode, foundNode, timeOutHandler));

      // Inform Local Statistic PI to perform local statistic calculation
      activeLearningNode.requestDistributedSuggestions(this.splitId, this);
    }
  }

  /**
   * Helper method to continue the attempt to split once all local calculation results are received.
   * 
   * @param activeLearningNode
   *          The corresponding active learning node which will be split
   * @param foundNode
   *          The data structure to represents the filtering of the instance using the tree model.
   */
  private void continueAttemptToSplit(ActiveLearningNode activeLearningNode, FoundNode foundNode) {
    AttributeSplitSuggestion bestSuggestion = activeLearningNode.getDistributedBestSuggestion();
    AttributeSplitSuggestion secondBestSuggestion = activeLearningNode.getDistributedSecondBestSuggestion();

    // compare with null split
    double[] preSplitDist = activeLearningNode.getObservedClassDistribution();
    AttributeSplitSuggestion nullSplit = new AttributeSplitSuggestion(null, new double[0][],
        this.splitCriterion.getMeritOfSplit(preSplitDist, new double[][] { preSplitDist }));

    if ((bestSuggestion == null) || (nullSplit.compareTo(bestSuggestion) > 0)) {
      secondBestSuggestion = bestSuggestion;
      bestSuggestion = nullSplit;
    } else {
      if ((secondBestSuggestion == null) || (nullSplit.compareTo(secondBestSuggestion) > 0)) {
        secondBestSuggestion = nullSplit;
      }
    }

    boolean shouldSplit = false;

    if (secondBestSuggestion == null) {
      shouldSplit = (bestSuggestion != null);
    } else {
      double hoeffdingBound = computeHoeffdingBound(
          this.splitCriterion.getRangeOfMerit(activeLearningNode.getObservedClassDistribution()), this.splitConfidence,
          activeLearningNode.getWeightSeen());

      if ((bestSuggestion.merit - secondBestSuggestion.merit > hoeffdingBound) || (hoeffdingBound < tieThreshold)) {
        shouldSplit = true;
      }
      // TODO: add poor attributes removal
    }

    SplitNode parent = foundNode.getParent();
    int parentBranch = foundNode.getParentBranch();

    // split if the Hoeffding bound condition is satisfied
    if (shouldSplit) {

      if (bestSuggestion.splitTest != null) {
        SplitNode newSplit = new SplitNode(bestSuggestion.splitTest, activeLearningNode.getObservedClassDistribution());

        for (int i = 0; i < bestSuggestion.numSplits(); i++) {
          Node newChild = newLearningNode(bestSuggestion.resultingClassDistributionFromSplit(i), this.parallelismHint);
          newSplit.setChild(i, newChild);
        }

        this.activeLeafNodeCount--;
        this.decisionNodeCount++;
        this.activeLeafNodeCount += bestSuggestion.numSplits();

        if (parent == null) {
          this.treeRoot = newSplit;
        } else {
          parent.setChild(parentBranch, newSplit);
        }
      }
      // TODO: add check on the model's memory size
    }

    // housekeeping
    activeLearningNode.endSplitting();
    activeLearningNode.setWeightSeenAtLastSplitEvaluation(activeLearningNode.getWeightSeen());
  }

  /**
   * Helper method to deactivate learning node
   * 
   * @param toDeactivate
   *          Active Learning Node that will be deactivated
   * @param parent
   *          Parent of the soon-to-be-deactivated Active LearningNode
   * @param parentBranch
   *          the branch index of the node in the parent node
   */
  private void deactivateLearningNode(ActiveLearningNode toDeactivate, SplitNode parent, int parentBranch) {
    Node newLeaf = new InactiveLearningNode(toDeactivate.getObservedClassDistribution());
    if (parent == null) {
      this.treeRoot = newLeaf;
    } else {
      parent.setChild(parentBranch, newLeaf);
    }

    this.activeLeafNodeCount--;
    this.inactiveLeafNodeCount++;
  }

  private LearningNode newLearningNode(int parallelismHint) {
    return newLearningNode(new double[0], parallelismHint);
  }

  private LearningNode newLearningNode(double[] initialClassObservations, int parallelismHint) {
    // for VHT optimization, we need to dynamically instantiate the appropriate
    // ActiveLearningNode
    return new ActiveLearningNode(initialClassObservations, parallelismHint);
  }

  /**
   * Helper method to set the model context, i.e. how many attributes they are and what is the class index
   * 
   * @param ih
   */
  private void setModelContext(InstancesHeader ih) {
    // TODO possibly refactored
    if ((ih != null) && (ih.classIndex() < 0)) {
      throw new IllegalArgumentException("Context for a classifier must include a class to learn");
    }
    // TODO: check flag for checking whether training has started or not

    // model context is used to describe the model
    logger.trace("Model context: {}", ih.toString());
  }

  private static double computeHoeffdingBound(double range, double confidence, double n) {
    return Math.sqrt((Math.pow(range, 2.0) * Math.log(1.0 / confidence)) / (2.0 * n));
  }

  /**
   * AggregationTimeOutHandler is a class to support time-out feature while waiting for local computation results from
   * the local statistic PIs.
   * 
   * @author Arinto Murdopo
   * 
   */
  static class AggregationTimeOutHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AggregationTimeOutHandler.class);
    private final Long splitId;
    private final BlockingQueue<Long> toBeSplittedNodes;

    AggregationTimeOutHandler(Long splitId, BlockingQueue<Long> toBeSplittedNodes) {
      this.splitId = splitId;
      this.toBeSplittedNodes = toBeSplittedNodes;
    }

    @Override
    public void run() {
      logger.debug("Time out is reached. AggregationTimeOutHandler is started.");
      try {
        toBeSplittedNodes.put(splitId);
      } catch (InterruptedException e) {
        logger.warn("Interrupted while trying to put the ID into the queue");
      }
      logger.debug("AggregationTimeOutHandler is finished.");
    }
  }

  /**
   * SplittingNodeInfo is a class to represents the ActiveLearningNode that is splitting
   * 
   * @author Arinto Murdopo
   * 
   */
  static class SplittingNodeInfo implements Serializable {

    private final ActiveLearningNode activeLearningNode;
    private final FoundNode foundNode;
    private final transient ScheduledFuture<?> scheduledFuture;

    SplittingNodeInfo(ActiveLearningNode activeLearningNode, FoundNode foundNode, ScheduledFuture<?> scheduledFuture) {
      this.activeLearningNode = activeLearningNode;
      this.foundNode = foundNode;
      this.scheduledFuture = scheduledFuture;
    }
  }

  protected ChangeDetector changeDetector;

  public ChangeDetector getChangeDetector() {
    return this.changeDetector;
  }

  public void setChangeDetector(ChangeDetector cd) {
    this.changeDetector = cd;
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

    // default values
    private SplitCriterion splitCriterion = new InfoGainSplitCriterion();
    private double splitConfidence = 0.0000001;
    private double tieThreshold = 0.05;
    private int gracePeriod = 200;
    private int parallelismHint = 1;
    private long timeOut = 30;
    private ChangeDetector changeDetector = null;

    Builder(Instances dataset) {
      this.dataset = dataset;
    }

    Builder(ModelAggregatorProcessor oldProcessor) {
      this.dataset = oldProcessor.dataset;
      this.splitCriterion = oldProcessor.splitCriterion;
      this.splitConfidence = oldProcessor.splitConfidence;
      this.tieThreshold = oldProcessor.tieThreshold;
      this.gracePeriod = oldProcessor.gracePeriod;
      this.parallelismHint = oldProcessor.parallelismHint;
      this.timeOut = oldProcessor.timeOut;
    }

    Builder splitCriterion(SplitCriterion splitCriterion) {
      this.splitCriterion = splitCriterion;
      return this;
    }

    Builder splitConfidence(double splitConfidence) {
      this.splitConfidence = splitConfidence;
      return this;
    }

    Builder tieThreshold(double tieThreshold) {
      this.tieThreshold = tieThreshold;
      return this;
    }

    Builder gracePeriod(int gracePeriod) {
      this.gracePeriod = gracePeriod;
      return this;
    }

    Builder parallelismHint(int parallelismHint) {
      this.parallelismHint = parallelismHint;
      return this;
    }

    Builder timeOut(long timeOut) {
      this.timeOut = timeOut;
      return this;
    }

    Builder changeDetector(ChangeDetector changeDetector) {
      this.changeDetector = changeDetector;
      return this;
    }

    ModelAggregatorProcessor build() {
      return new ModelAggregatorProcessor(this);
    }
  }

}
