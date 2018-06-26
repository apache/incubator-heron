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

#include "gtest/gtest.h"

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"

#include "metrics/metrics.h"

namespace heron {
namespace common {

class MultiMeanMetricTest : public ::testing::Test {
 public:
  MultiMeanMetricTest() {}
  ~MultiMeanMetricTest() {}

  void SetUp() { multi_mean_metric_ = new MultiMeanMetric(); }

  void TearDown() { delete multi_mean_metric_; }

  heron::proto::system::MetricPublisherPublishMessage* CreateEmptyPublishMessage() {
    return new heron::proto::system::MetricPublisherPublishMessage();
  }

 protected:
  MultiMeanMetric* multi_mean_metric_;
};

TEST_F(MultiMeanMetricTest, testOneMeanMetric) {
  sp_string scope1 = "testscope1";
  MeanMetric* mean_metric = multi_mean_metric_->scope(scope1);
  mean_metric->record(5);

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  double expectedMean = 5.0;

  // Prefix should also have scope attached to it.
  sp_string expectedPrefix = prefix + "/" + scope1;

  multi_mean_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_DOUBLE_EQ(expectedMean, atof(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(MultiMeanMetricTest, testMultipleMeanMetrics) {
  sp_string scope1 = "testscope1";
  MeanMetric* mean_metric1 = multi_mean_metric_->scope(scope1);
  mean_metric1->record(5);
  mean_metric1->record(2);

  sp_string scope2 = "testscope2";
  MeanMetric* mean_metric2 = multi_mean_metric_->scope(scope2);
  mean_metric2->record(5);
  mean_metric2->record(8);
  mean_metric2->record(2);

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  double expectedMean1 = 3.5;
  double expectedMean2 = 5.0;
  sp_string expectedPrefix1 = prefix + "/" + scope1;
  sp_string expectedPrefix2 = prefix + "/" + scope2;

  multi_mean_metric_->GetAndReset(prefix, message);

  // Two metric datum should be present.
  EXPECT_EQ(2, message->metrics_size());

  // Check first datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix1.c_str(), datum.name().c_str());
  EXPECT_DOUBLE_EQ(expectedMean1, atof(datum.value().c_str()));

  // Check second datum.
  datum = message->metrics(1);
  EXPECT_STREQ(expectedPrefix2.c_str(), datum.name().c_str());
  EXPECT_DOUBLE_EQ(expectedMean2, atof(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(MultiMeanMetricTest, testGetAndReset) {
  sp_string scope1 = "testscope1";
  MeanMetric* mean_metric1 = multi_mean_metric_->scope(scope1);
  mean_metric1->record(2);
  mean_metric1->record(4);

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  double expectedMean = 3.0;
  sp_string expectedPrefix = prefix + "/" + scope1;

  multi_mean_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_DOUBLE_EQ(expectedMean, atof(datum.value().c_str()));

  // Clean up.
  delete message;

  // After last GetAndReset, the value should have been reset.
  // Create another message for next GetAndReset call.
  message = CreateEmptyPublishMessage();

  // Expected mean should be zero.
  expectedMean = 0.0;
  multi_mean_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_DOUBLE_EQ(expectedMean, atof(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(MultiMeanMetricTest, testSameScope) {
  sp_string scope1 = "testscope1";
  MeanMetric* mean_metric1 = multi_mean_metric_->scope(scope1);
  mean_metric1->record(4);
  mean_metric1->record(5);

  // Same scope
  sp_string scope2 = "testscope1";
  MeanMetric* mean_metric2 = multi_mean_metric_->scope(scope2);
  mean_metric2->record(9);

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";

  // Expected mean is sum of all the above records.
  double expectedMean = 6.0;
  sp_string expectedPrefix = prefix + "/" + scope1;

  multi_mean_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_DOUBLE_EQ(expectedMean, atof(datum.value().c_str()));

  // Clean up.
  delete message;
}
}  // namespace common
}  // namespace heron

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
