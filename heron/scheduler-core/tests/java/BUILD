load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "@com_google_guava_guava//jar",
    "@commons_io_commons_io//jar",
    "//third_party/java:powermock",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
    "//heron/scheduler-core/src/java:scheduler-java",
    "//third_party/java:junit4",
    "//third_party/java:mockito",
]

spi_deps_files = [
  "//heron/spi/src/java:common-spi-java",
  "//heron/spi/src/java:statemgr-spi-java",
  "//heron/spi/src/java:uploader-spi-java",
  "//heron/spi/src/java:packing-spi-java",
  "//heron/spi/src/java:scheduler-spi-java",
  "//heron/spi/src/java:utils-spi-java",
]

scheduler_deps_files = \
  common_deps_files + \
  heron_java_proto_files() + \
  spi_deps_files

java_library(
  name = "scheduler-core-tests",
  srcs = glob(["**/*.java"]),
  deps = scheduler_deps_files + [
    "//heron/statemgrs/src/java:null-statemgr-java",
    "//heron/schedulers/src/java:null-scheduler-java",
    "//heron/packing/src/java:roundrobin-packing",
  ],
)

java_tests(
  test_classes = [
    "com.twitter.heron.scheduler.LaunchRunnerTest",
    "com.twitter.heron.scheduler.RuntimeManagerRunnerTest",
    "com.twitter.heron.scheduler.RuntimeManagerMainTest",
    "com.twitter.heron.scheduler.SubmitterMainTest",
    "com.twitter.heron.scheduler.SchedulerMainTest",
    "com.twitter.heron.scheduler.UpdateTopologyManagerTest",
    "com.twitter.heron.scheduler.server.SchedulerServerTest",
    "com.twitter.heron.scheduler.client.LibrarySchedulerClientTest",
    "com.twitter.heron.scheduler.client.HttpServiceSchedulerClientTest",
    "com.twitter.heron.scheduler.client.SchedulerClientFactoryTest",
    "com.twitter.heron.scheduler.dryrun.SubmitDryRunRenderTest",
    "com.twitter.heron.scheduler.dryrun.UpdateDryRunRenderTest",
    "com.twitter.heron.scheduler.utils.LauncherUtilsTest",
    "com.twitter.heron.scheduler.utils.SchedulerUtilsTest",
  ],
  runtime_deps = [ ":scheduler-core-tests" ],
  resources = ["//heron/scheduler-core/tests/resources:scheduler-resources"],
  size = "small",
)
