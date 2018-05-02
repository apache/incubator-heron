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

class MultiCountMetricTest : public ::testing::Test {
 public:
  MultiCountMetricTest() {}
  ~MultiCountMetricTest() {}

  void SetUp() { multi_count_metric_ = new MultiCountMetric(); }

  void TearDown() { delete multi_count_metric_; }

  heron::proto::system::MetricPublisherPublishMessage* CreateEmptyPublishMessage() {
    return new heron::proto::system::MetricPublisherPublishMessage();
  }

 protected:
  MultiCountMetric* multi_count_metric_;
};

TEST_F(MultiCountMetricTest, testOneCountMetric) {
  sp_string scope1 = "testscope1";
  CountMetric* count_metric = multi_count_metric_->scope(scope1);
  count_metric->incr();

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 1;

  // Prefix should also have scope attached to it.
  sp_string expectedPrefix = prefix + "/" + scope1;

  multi_count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(MultiCountMetricTest, testMultipleCountMetrics) {
  sp_string scope1 = "testscope1";
  CountMetric* count_metric1 = multi_count_metric_->scope(scope1);
  count_metric1->incr();
  count_metric1->incr();

  sp_string scope2 = "testscope2";
  CountMetric* count_metric2 = multi_count_metric_->scope(scope2);
  count_metric2->incr_by(3);

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount1 = 2;
  int expectedCount2 = 3;
  sp_string expectedPrefix1 = prefix + "/" + scope1;
  sp_string expectedPrefix2 = prefix + "/" + scope2;

  multi_count_metric_->GetAndReset(prefix, message);

  // Two metric datum should be present.
  EXPECT_EQ(2, message->metrics_size());

  // Check first datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix1.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount1, atoi(datum.value().c_str()));

  // Check second datum.
  datum = message->metrics(1);
  EXPECT_STREQ(expectedPrefix2.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount2, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(MultiCountMetricTest, testGetAndReset) {
  sp_string scope1 = "testscope1";
  CountMetric* count_metric1 = multi_count_metric_->scope(scope1);
  count_metric1->incr_by(2);

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 2;
  sp_string expectedPrefix = prefix + "/" + scope1;

  multi_count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;

  // After last GetAndReset, the value should have been reset.
  // Create another message for next GetAndReset call.
  message = CreateEmptyPublishMessage();

  // Expected count should be zero.
  expectedCount = 0;
  multi_count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(MultiCountMetricTest, testSameScope) {
  sp_string scope1 = "testscope1";
  CountMetric* count_metric1 = multi_count_metric_->scope(scope1);
  count_metric1->incr();
  count_metric1->incr();

  // Same scope
  sp_string scope2 = "testscope1";
  CountMetric* count_metric2 = multi_count_metric_->scope(scope2);
  count_metric2->incr_by(3);

  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";

  // Expected count is sum of all the above increments.
  int expectedCount = 5;
  sp_string expectedPrefix = prefix + "/" + scope1;

  multi_count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(expectedPrefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  delete message;
}
}  // namespace common
}  // namespace heron

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
