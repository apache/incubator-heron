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


package org.apache.heron.examples.streamlet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;

/**
 * This topology demonstrates how different streamlets can be united into one
 * using union operations. Wire requests (customers asking to wire money) are
 * created for three different bank branches. Each bank branch is a separate
 * streamlet. In each branch's streamlet, the request amount is checked to make
 * sure that no one requests a wire transfer of more than $500. Then, the
 * wire request streamlets for all three branches are combined into one using a
 * union operation. Each element in the unified streamlet then passes through a
 * fraud detection filter that ensures that no "bad" customers are allowed to
 * make requests.
 */
public final class WireRequestsTopology {
  private WireRequestsTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(WireRequestsTopology.class.getName());

  /**
   * A list of current customers (some good, some bad).
   */
  private static final List<String> CUSTOMERS = Arrays.asList(
      "honest-tina",
      "honest-jeff",
      "scheming-dave",
      "scheming-linda"
  );

  /**
   * A list of bad customers whose requests should be rejected.
   */
  private static final List<String> FRAUDULENT_CUSTOMERS = Arrays.asList(
      "scheming-dave",
      "scheming-linda"
  );

  /**
   * The maximum allowable amount for transfers. Requests for more than this
   * amount need to be rejected.
   */
  private static final int MAX_ALLOWABLE_AMOUNT = 500;

  /**
   * A POJO for wire requests.
   */
  private static class WireRequest implements Serializable {
    private static final long serialVersionUID = 1311441220738558016L;
    private String customerId;
    private int amount;

    WireRequest(long delay) {
      // The pace at which requests are generated is throttled. Different
      // throttles are applied to different bank branches.
      StreamletUtils.sleep(delay);
      this.customerId = StreamletUtils.randomFromList(CUSTOMERS);
      this.amount = ThreadLocalRandom.current().nextInt(1000);
      LOG.info(String.format("New wire request: %s", this));
    }

    String getCustomerId() {
      return customerId;
    }

    int getAmount() {
      return amount;
    }

    @Override
    public String toString() {
      return String.format("(customer: %s, amount: %d)", customerId, amount);
    }
  }

  /**
   * Each request is checked to make sure that requests from untrustworthy customers
   * are rejected.
   */
  private static boolean fraudDetect(WireRequest request) {
    String logMessage;

    boolean fraudulent = FRAUDULENT_CUSTOMERS.contains(request.getCustomerId());

    if (fraudulent) {
      logMessage = String.format("Rejected fraudulent customer %s",
          request.getCustomerId());
      LOG.warning(logMessage);
    } else {
      logMessage = String.format("Accepted request for $%d from customer %s",
          request.getAmount(),
          request.getCustomerId());
      LOG.info(logMessage);
    }

    return !fraudulent;
  }

  /**
   * Each request is checked to make sure that no one requests an amount over $500.
   */
  private static boolean checkRequestAmount(WireRequest request) {
    boolean sufficientBalance = request.getAmount() < MAX_ALLOWABLE_AMOUNT;

    if (!sufficientBalance) {
      LOG.warning(
          String.format("Rejected excessive request of $%d",
              request.getAmount()));
    }

    return sufficientBalance;
  }

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder builder = Builder.newBuilder();

    // Requests from the "quiet" bank branch (high throttling).
    Streamlet<WireRequest> quietBranch = builder.newSource(() -> new WireRequest(20))
        .setNumPartitions(1)
        .setName("quiet-branch-requests")
        .filter(WireRequestsTopology::checkRequestAmount)
        .setName("quiet-branch-check-balance");

    // Requests from the "medium" bank branch (medium throttling).
    Streamlet<WireRequest> mediumBranch = builder.newSource(() -> new WireRequest(10))
        .setNumPartitions(2)
        .setName("medium-branch-requests")
        .filter(WireRequestsTopology::checkRequestAmount)
        .setName("medium-branch-check-balance");

    // Requests from the "busy" bank branch (low throttling).
    Streamlet<WireRequest> busyBranch = builder.newSource(() -> new WireRequest(5))
        .setNumPartitions(4)
        .setName("busy-branch-requests")
        .filter(WireRequestsTopology::checkRequestAmount)
        .setName("busy-branch-check-balance");

    // Here, the streamlets for the three bank branches are united into one. The fraud
    // detection filter then operates on that unified streamlet.
    quietBranch
        .union(mediumBranch)
        .setNumPartitions(2)
        .setName("union-1")
        .union(busyBranch)
        .setName("union-2")
        .setNumPartitions(4)
        .filter(WireRequestsTopology::fraudDetect)
        .setName("all-branches-fraud-detect")
        .log();

    Config config = Config.newBuilder()
        .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE)
        .setNumContainers(2)
        .build();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder);
  }
}
