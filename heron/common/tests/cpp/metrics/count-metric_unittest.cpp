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

class CountMetricTest : public ::testing::Test {
 public:
  CountMetricTest() {}
  ~CountMetricTest() {}

  void SetUp() { count_metric_ = new CountMetric(); }

  void TearDown() { delete count_metric_; }

  heron::proto::system::MetricPublisherPublishMessage* CreateEmptyPublishMessage() {
    return new heron::proto::system::MetricPublisherPublishMessage();
  }

 protected:
  CountMetric* count_metric_;
};

TEST_F(CountMetricTest, testIncr) {
  count_metric_->incr();
  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 1;
  count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(CountMetricTest, testMultipleIncr) {
  count_metric_->incr();
  count_metric_->incr();
  count_metric_->incr();
  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 3;
  count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(CountMetricTest, testIncrBy) {
  count_metric_->incr_by(2);
  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 2;
  count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(CountMetricTest, testIncrByAndIncr) {
  count_metric_->incr_by(2);
  count_metric_->incr_by(3);
  count_metric_->incr();
  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 6;
  count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(CountMetricTest, testGetAndReset) {
  count_metric_->incr();
  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 1;
  count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;

  // After last GetAndReset, the value should have been reset.
  // Create another message for next GetAndReset call.
  message = CreateEmptyPublishMessage();

  // Expected count should be zero.
  expectedCount = 0;
  count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(CountMetricTest, testMultipleDatum) {
  count_metric_->incr();
  heron::proto::system::MetricPublisherPublishMessage* message = CreateEmptyPublishMessage();

  sp_string prefix = "TestPrefix";
  int expectedCount = 1;
  count_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  heron::proto::system::MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Next metrics
  count_metric_->incr_by(2);

  // Send the same message object.
  count_metric_->GetAndReset(prefix, message);

  // Two metric datum should be present.
  EXPECT_EQ(2, message->metrics_size());

  // Check the first datum.
  datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

  // Check the second datum.
  expectedCount = 2;
  datum = message->metrics(1);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());
  EXPECT_EQ(expectedCount, atoi(datum.value().c_str()));

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
