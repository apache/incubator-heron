/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.spouts.kafka;

import com.twitter.heron.api.metric.ICombiner;
import com.twitter.heron.api.metric.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class KafkaMetric {
    // Utility class. Not to be instantiated.
    private KafkaMetric() {
    }

    // MaxMetric is used for max latency type metrics.
    public static class MaxMetric implements ICombiner<Long> {
        @Override
        public Long identity() {
            return new Long(0);
        }

        @Override
        public Long combine(Long l1, Long l2) {
            if (l1 == null) {
                return l2;
            }
            if (l2 == null) {
                return l1;
            }
            return Math.max(l1, l2);
        }
    }

    // PartitionManager metrics map will be connected to this
    public static class PartitionMetric implements IMetric {
        public static final Logger LOG = LoggerFactory.getLogger(PartitionMetric.class);
        private PartitionCoordinator coordinator;

        public PartitionMetric(PartitionCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public Object getValueAndReset() {
            Map ret = new HashMap();
            try {
                List<PartitionManager> pms = coordinator.getMyManagedPartitions();
                for (PartitionManager pm : pms) {
                    ret.putAll(pm.getMetricsDataMap());
                }
            } catch (Exception e) {
                LOG.warn("Metrics Tick: Exception when computing partition metric.", e);
                ret.put(String.format("metricException/%s", e.getClass().getSimpleName()), 1);
            }
            return ret;
        }
    }

    // OffsetMetric is used for storing kafka partition offset related data.
    public static class OffsetMetric implements IMetric {
        public static final Logger LOG = LoggerFactory.getLogger(OffsetMetric.class);
        private long lastCalculatedTotalSpoutLag = 0;
        private PartitionCoordinator coordinator;

        /**
         * Ctor
         */
        public void setCoordinator(PartitionCoordinator zkCoordinator) {
            coordinator = zkCoordinator;
        }

        public long getLastCalculatedTotalSpoutLag() {
            return lastCalculatedTotalSpoutLag;
        }

        @Override
        public Object getValueAndReset() {
            // Time related metrics are commented out because of new consumer specifics
            long totalSpoutLag = 0;
            long totalLatestTimeOffset = 0;
            long totalLatestEmittedOffset = 0;
            long totalLatestCommittedOffset = 0;
            HashMap ret = new HashMap();
            try {
                List<PartitionManager> managedPartitions = coordinator.getMyManagedPartitions();
                for (PartitionManager partitionManager : managedPartitions) {
                    /*long latestTimeOffset =
                            partitionManager.getOffsetForTimestamp(OffsetRequest.LatestTime());*/
                    long latestEmittedOffset = partitionManager.lastCompletedOffset();
                    long latestCommittedOffset = partitionManager.getCommitedTo();
                    //long spoutLag = latestTimeOffset - latestEmittedOffset;
                    //totalSpoutLag += spoutLag;
                    //totalLatestTimeOffset += latestTimeOffset;
                    totalLatestEmittedOffset += latestEmittedOffset;
                    totalLatestCommittedOffset += latestCommittedOffset;
                    String strPartitionId = partitionManager.getPartitionId().toString();
                    //ret.put(String.format("%s/spoutLag", strPartitionId), spoutLag);
                    //ret.put(String.format("%s/latestTime", strPartitionId), latestTimeOffset);
                    ret.put(String.format("%s/latestEmittedOffset", strPartitionId), latestEmittedOffset);
                    ret.put(String.format("%s/latestCommittedOffset", strPartitionId), latestCommittedOffset);
                }
                //ret.put("totalSpoutLag", totalSpoutLag);
                //ret.put("totalLatestTime", totalLatestTimeOffset);
                ret.put("totalLatestEmittedOffset", totalLatestEmittedOffset);
                ret.put("totalLatestCommittedOffset", totalLatestCommittedOffset);
                //lastCalculatedTotalSpoutLag = totalSpoutLag;
            } catch (Exception e) {
                LOG.warn("Metrics Tick: Exception when computing kafkaOffset metric.", e);
                ret.put(String.format("metricException/%s", e.getClass().getSimpleName()), 1);
            }
            return ret;
        }
    }
}
