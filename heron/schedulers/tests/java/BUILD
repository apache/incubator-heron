load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "@guava//jar",
    "//3rdparty/java:powermock",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
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
    heron_java_proto_files() + \
    common_deps_files + \
    spi_deps_files

aurora_deps_files = [
    "//heron/schedulers/src/java:aurora-scheduler-java",
]

yarn_deps_files = [
    "//heron/schedulers/src/java:yarn-scheduler-java",
    "@reef-common//jar",
    "@tang//jar",
    "@wake//jar",
    "@reef-runtime-local//jar",
    "@javax.inject//jar",
    "@avro//jar",
    "@commons-lang//jar",
    "@netty-all//jar",
    "@jackson-core-asl//jar",
    "@jackson-mapper-asl//jar",
]

local_deps_files = [
    "//heron/schedulers/src/java:local-scheduler-java",
]

slurm_deps_files = [
    "//heron/schedulers/src/java:slurm-scheduler-java",
]

java_test(
  name="AuroraSchedulerTest",
  srcs=glob(
    ["**/AuroraSchedulerTest.java"]
  ),
  deps=scheduler_deps_files + aurora_deps_files,
  size="small",
)

java_test(
  name="AuroraLauncherTest",
  srcs=glob(
    ["**/AuroraLauncherTest.java"]
  ),
  deps=scheduler_deps_files + aurora_deps_files,
  size="small",
)

java_test(
  name="AuroraControllerTest",
  srcs=glob(
    ["**/AuroraControllerTest.java"]
  ),
  deps=scheduler_deps_files + aurora_deps_files,
  size="small",
)

java_test(
  name="yarn-scheduler_unittest",
  srcs=glob([
      "**/YarnSchedulerTest.java",
      "**/HeronMasterDriverTest.java",
      "**/DriverOnLocalYarnTest.java",
      "**/HeronExecutorTaskTest.java",
    ]
  ),
  deps=scheduler_deps_files + yarn_deps_files,
  size="small",
)

java_test(
  name="LocalLauncherTest",
  srcs=glob(
    ["**/LocalLauncherTest.java"]
  ),
  deps=scheduler_deps_files + local_deps_files,
  size="small",
)

java_test(
  name="LocalSchedulerTest",
  srcs=glob(
    ["**/LocalSchedulerTest.java"]
  ),
  deps=scheduler_deps_files + local_deps_files,
  size="small",
)

java_test(
  name="SlurmLauncherTest",
  srcs=glob(
    ["**/SlurmLauncherTest.java"]
  ),
  deps=scheduler_deps_files + slurm_deps_files,
  size="small",
)

java_test(
  name="SlurmSchedulerTest",
  srcs=glob(
    ["**/SlurmSchedulerTest.java"]
  ),
  deps=scheduler_deps_files + slurm_deps_files,
  size="small",
)

java_test(
  name="SlurmControllerTest",
  srcs=glob(
    ["**/SlurmControllerTest.java"]
  ),
  deps=scheduler_deps_files + slurm_deps_files,
  size="small",
)
