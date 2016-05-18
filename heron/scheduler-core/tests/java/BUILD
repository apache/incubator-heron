load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "@guava//jar",
    "//3rdparty/java:powermock",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
    "//heron/scheduler-core/src/java:scheduler-java",
    "@bazel_tools//third_party:junit4",
    "@bazel_tools//third_party:mockito",
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

# Following unit tests are for classes under package com.twitter.heron.scheduler
java_test(
  name="launch-runner_unittest",
  srcs=glob(
    ["**/LaunchRunnerTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="runtime-manager-runner_unittest",
  srcs=glob(
    ["**/RuntimeManagerRunnerTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="runtime-manager-main_unittest",
  srcs=glob(
    ["**/RuntimeManagerMainTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files  +
    ["//heron/statemgrs/src/java:null-statemgr-java"],
  size="small",
)

java_test(
  name="submitter-main_unittest",
  srcs=glob(
    ["**/SubmitterMainTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="scheduler-main_unittest",
  srcs=glob(
    ["**/SchedulerMainTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

# Following unit tests are for classes under package com.twitter.heron.scheduler.server
java_test(
  name="scheduler-server_unittest",
  srcs=glob(
    ["**/SchedulerServerTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)


# Following unit tests are for class under package com.twitter.heron.scheduler.client

java_test(
  name="library_scheduler_client_unittest",
  srcs=glob(
    ["**/LibrarySchedulerClientTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="http_service_scheduler_client_unittest",
  srcs=glob(
    ["**/HttpServiceSchedulerClientTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="scheduler_client_factory_unittest",
  srcs=glob(
    ["**/SchedulerClientFactoryTest.java"]
  ),
  deps=scheduler_deps_files +
    ["//heron/schedulers/src/java:null-scheduler-java"],
  size="small",
)
