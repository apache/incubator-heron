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

using namespace std::chrono;
using namespace heron::common;
using namespace heron::proto::system;

class TimeSpentMetricTest : public ::testing::Test
{
 public:
  TimeSpentMetricTest() { }
  ~TimeSpentMetricTest() { }

  void SetUp()
  {
    time_spent_metric_ = new TimeSpentMetric();
  }

  void TearDown()
  {
    delete time_spent_metric_;
  }

  // It is hard to test the time in milliseconds.
  // So we use EXPECT_NEAR, which take the error margin
  // as a parameter.
  // This function returns the absolute error expected
  // for the given expected time.
  sp_lint32 ExpectedError(sp_lint32 expectedTime)
  {
    // We expect only 5% of the expected value as error.
    sp_lint32 error = (sp_lint32) (5.0 / 100.0 * expectedTime);
    return error;
  }

 protected:
  TimeSpentMetric*      time_spent_metric_;
};

TEST_F(TimeSpentMetricTest, testStart)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  auto start = high_resolution_clock::now();
  time_spent_metric_->Start();

  MetricPublisherPublishMessage* message = new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  // Sleep for some time
  ::usleep(sleepTime);

  auto end = high_resolution_clock::now();
  time_spent_metric_->GetAndReset(prefix, message);

  sp_lint32 expectedTime = duration_cast<milliseconds>(end - start).count();

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime = atol(datum.value().c_str());
  sp_lint32 expectedError = ExpectedError(expectedTime);
  EXPECT_NEAR(expectedTime, actualTime, expectedError);

  // Clean up.
  delete message;
}

TEST_F(TimeSpentMetricTest, testStartWithoutStop)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  auto start = high_resolution_clock::now();
  time_spent_metric_->Start();

  MetricPublisherPublishMessage* message =
    new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  // Sleep for some time
  ::usleep(sleepTime);

  // Starting again should not make any difference.
  time_spent_metric_->Start();

  // It will just add up in the current recorded time.
  ::usleep(sleepTime);

  auto end = high_resolution_clock::now();
  time_spent_metric_->GetAndReset(prefix, message);

  sp_lint32 expectedTime = duration_cast<milliseconds>(end - start).count();

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime = atol(datum.value().c_str());
  sp_lint32 expectedError = ExpectedError(expectedTime);
  EXPECT_NEAR(expectedTime, actualTime, expectedError);

  // Clean up.
  delete message;
}

TEST_F(TimeSpentMetricTest, testStopWithouStart)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  MetricPublisherPublishMessage* message =
    new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  // Sleep for some time just to check.
  ::usleep(sleepTime);

  // This should do nothing.
  time_spent_metric_->Stop();

  time_spent_metric_->GetAndReset(prefix, message);

  sp_lint32 expectedTime = 0l;

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime = atol(datum.value().c_str());
  sp_lint32 expectedError = ExpectedError(expectedTime);
  EXPECT_NEAR(expectedTime, actualTime, expectedError);

  // Clean up.
  delete message;
}

TEST_F(TimeSpentMetricTest, testStopAfterStop)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  auto start = high_resolution_clock::now();
  time_spent_metric_->Start();

  MetricPublisherPublishMessage* message =
    new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  // Sleep for some time
  ::usleep(sleepTime);

  auto end = high_resolution_clock::now();
  time_spent_metric_->Stop();

  // It will should not affect the end time...
  ::usleep(sleepTime);

  // if we Stop after a Stop.
  time_spent_metric_->Stop();

  time_spent_metric_->GetAndReset(prefix, message);
  sp_lint32 expectedTime = duration_cast<milliseconds>(end - start).count();

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime = atol(datum.value().c_str());
  sp_lint32 expectedError = ExpectedError(expectedTime);
  EXPECT_NEAR(expectedTime, actualTime, expectedError);

  // Clean up.
  delete message;
}

TEST_F(TimeSpentMetricTest, testStartStopStart)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  auto start = high_resolution_clock::now();
  time_spent_metric_->Start();

  // Sleep for some time
  ::usleep(sleepTime);

  auto end = high_resolution_clock::now();
  time_spent_metric_->Stop();

  sp_lint32 expectedTime = duration_cast<milliseconds>(end - start).count();

  // Start again
  start = high_resolution_clock::now();
  time_spent_metric_->Start();

  MetricPublisherPublishMessage* message =
    new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  // Sleep for some time
  ::usleep(sleepTime);

  end = high_resolution_clock::now();
  time_spent_metric_->GetAndReset(prefix, message);

  expectedTime += duration_cast<milliseconds>(end - start).count();

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime = atol(datum.value().c_str());
  sp_lint32 expectedError = ExpectedError(expectedTime);
  EXPECT_NEAR(expectedTime, actualTime, expectedError);

  // Clean up.
  delete message;
}

TEST_F(TimeSpentMetricTest, testMultipleStartStops)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  auto start = high_resolution_clock::now();
  time_spent_metric_->Start();

  // Sleep for some time
  ::usleep(sleepTime);

  auto end = high_resolution_clock::now();
  time_spent_metric_->Stop();

  sp_lint32 expectedTime = duration_cast<milliseconds>(end - start).count();

  // Do it again.
  start = high_resolution_clock::now();
  time_spent_metric_->Start();

  // Sleep for some time
  ::usleep(sleepTime);

  end = high_resolution_clock::now();
  time_spent_metric_->Stop();

  expectedTime += duration_cast<milliseconds>(end - start).count();

  MetricPublisherPublishMessage* message =
    new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  time_spent_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime = atol(datum.value().c_str());
  sp_lint32 expectedError = ExpectedError(expectedTime);
  EXPECT_NEAR(expectedTime, actualTime, expectedError);

  // Clean up.
  delete message;
}

TEST_F(TimeSpentMetricTest, testGetAndReset)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  auto start = high_resolution_clock::now();
  time_spent_metric_->Start();

  // Sleep for some time
  ::usleep(sleepTime);

  auto end = high_resolution_clock::now();
  time_spent_metric_->Stop();

  sp_lint32 expectedTime = duration_cast<milliseconds>(end - start).count();

  MetricPublisherPublishMessage* message =
    new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  time_spent_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime = atol(datum.value().c_str());
  sp_lint32 expectedError = ExpectedError(expectedTime);
  EXPECT_NEAR(expectedTime, actualTime, expectedError);

  // Clean up.
  delete message;

  // After last GetAndReset, the value should have been reset.
  // Create another message for next GetAndReset call.
  message = new MetricPublisherPublishMessage();

  // Expected count should be zero.
  expectedTime = 0l;
  time_spent_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  // It should be absolutely zero.
  EXPECT_EQ(expectedTime, atol(datum.value().c_str()));

  // Clean up.
  delete message;
}

TEST_F(TimeSpentMetricTest, testMultipleDatum)
{
  sp_lint32 sleepTime = 100000;  // microseconds

  auto start = high_resolution_clock::now();
  time_spent_metric_->Start();

  // Sleep for some time
  ::usleep(sleepTime);

  auto end = high_resolution_clock::now();
  time_spent_metric_->Stop();

  sp_lint32 expectedTime1 = duration_cast<milliseconds>(end - start).count();

  MetricPublisherPublishMessage* message =
    new MetricPublisherPublishMessage();

  sp_string prefix = "TestPrefix";

  time_spent_metric_->GetAndReset(prefix, message);

  // Only one metric datum should be present.
  EXPECT_EQ(1, message->metrics_size());

  // Check that datum.
  MetricDatum datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime1 = atol(datum.value().c_str());
  sp_lint32 expectedError1 = ExpectedError(expectedTime1);
  EXPECT_NEAR(expectedTime1, actualTime1, expectedError1);

  // Next metrics
  // Do it again.
  start = high_resolution_clock::now();
  time_spent_metric_->Start();

  // Sleep for some time
  ::usleep(sleepTime);

  end= high_resolution_clock::now();
  time_spent_metric_->Stop();

  sp_lint32 expectedTime2 = duration_cast<milliseconds>(end - start).count();

  // Send the same message object.
  time_spent_metric_->GetAndReset(prefix, message);

  // Two metric datum should be present.
  EXPECT_EQ(2, message->metrics_size());

  // Check the first datum.
  datum = message->metrics(0);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  actualTime1 = atol(datum.value().c_str());
  expectedError1 = ExpectedError(expectedTime1);
  EXPECT_NEAR(expectedTime1, actualTime1, expectedError1);

  // Check the second datum.
  datum = message->metrics(1);
  EXPECT_STREQ(prefix.c_str(), datum.name().c_str());

  sp_lint32 actualTime2 = atol(datum.value().c_str());
  sp_lint32 expectedError2 = ExpectedError(expectedTime2);
  EXPECT_NEAR(expectedTime2, actualTime2, expectedError2);

  // Clean up.
  delete message;
}

sp_int32
main(sp_int32 argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
