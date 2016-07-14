load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "@com_google_guava_guava//jar",
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

# Following unit tests are for classes under package com.twitter.heron.scheduler
java_test(
  name="LaunchRunnerTest",
  srcs=glob(
    ["**/LaunchRunnerTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="RuntimeManagerRunnerTest",
  srcs=glob(
    ["**/RuntimeManagerRunnerTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="RuntimeManagerMainTest",
  srcs=glob(
    ["**/RuntimeManagerMainTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files  +
    ["//heron/statemgrs/src/java:null-statemgr-java"],
  size="small",
)

java_test(
  name="SubmitterMainTest",
  srcs=glob(
    ["**/SubmitterMainTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="SchedulerMainTest",
  srcs=glob(
    ["**/SchedulerMainTest.java"] +
    ["**/util/TopologyUtilityTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

# Following unit tests are for classes under package com.twitter.heron.scheduler.server
java_test(
  name="SchedulerServerTest",
  srcs=glob(
    ["**/SchedulerServerTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)


# Following unit tests are for class under package com.twitter.heron.scheduler.client

java_test(
  name="LibrarySchedulerClientTest",
  srcs=glob(
    ["**/LibrarySchedulerClientTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="HttpServiceSchedulerClientTest",
  srcs=glob(
    ["**/HttpServiceSchedulerClientTest.java"]
  ),
  deps=scheduler_deps_files,
  size="small",
)

java_test(
  name="SchedulerClientFactoryTest",
  srcs=glob(
    ["**/SchedulerClientFactoryTest.java"]
  ),
  deps=scheduler_deps_files +
    ["//heron/schedulers/src/java:null-scheduler-java"],
  size="small",
)
