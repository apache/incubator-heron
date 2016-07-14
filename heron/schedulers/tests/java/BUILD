load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "@com_google_guava_guava//jar",
    "//third_party/java:powermock",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
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
    heron_java_proto_files() + \
    common_deps_files + \
    spi_deps_files

aurora_deps_files = [
    "//heron/schedulers/src/java:aurora-scheduler-java",
]

yarn_deps_files = [
    "//heron/schedulers/src/java:yarn-scheduler-java",
    "@org_apache_reef_reef_common//jar",
    "@org_apache_reef_tang//jar",
    "@org_apache_reef_wake//jar",
    "@org_apache_reef_reef_runtime_local//jar",
    "@javax_inject_javax_inject//jar",
    "@org_apache_avro_avro//jar",
    "@commons_lang_commons_lang//jar",
    "@io_netty_netty_all//jar",
    "@org_codehaus_jackson_jackson_core_asl//jar",
    "@org_codehaus_jackson_jackson_mapper_asl//jar",
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
  name="YarnSchedulerTest",
  srcs=glob(["**/YarnSchedulerTest.java"]
  ),
  deps=scheduler_deps_files + yarn_deps_files,
  size="small",
)

java_test(
  name="HeronMasterDriverTest",
  srcs=glob(["**/HeronMasterDriverTest.java"]
  ),
  deps=scheduler_deps_files + yarn_deps_files,
  size="small",
)

java_test(
  name="DriverOnLocalReefTest",
  srcs=glob(["**/DriverOnLocalReefTest.java"]
  ),
  deps=scheduler_deps_files + yarn_deps_files,
  size="small",
)

java_test(
  name="HeronExecutorTaskTest",
  srcs=glob(["**/HeronExecutorTaskTest.java"]
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
