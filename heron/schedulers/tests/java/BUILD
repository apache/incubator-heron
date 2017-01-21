load("/tools/rules/java_tests", "java_tests")
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
    heron_java_proto_files() + \
    common_deps_files + \
    spi_deps_files

aurora_deps_files = [
    "//heron/schedulers/src/java:aurora-scheduler-java",
]

yarn_deps_files = [
    "//heron/packing/src/java:roundrobin-packing",
    "//heron/schedulers/src/java:yarn-scheduler-java",
    "@org_apache_reef_reef_common//jar",
    "@org_apache_reef_tang//jar",
    "@org_apache_reef_wake//jar",
    "@org_apache_reef_reef_runtime_local//jar",
    "@org_apache_reef_reef_runtime_yarn//jar",
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

mesos_deps_files = [
    "//heron/schedulers/src/java:mesos-scheduler-java",
    "@org_apache_mesos_mesos//jar",
]

slurm_deps_files = [
    "//heron/schedulers/src/java:slurm-scheduler-java",
]

marathon_deps_files = [
    "//heron/schedulers/src/java:marathon-scheduler-java",
]

java_library(
  name = "aurora-tests",
  srcs = glob(["**/aurora/*.java"]),
  deps = scheduler_deps_files + aurora_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.scheduler.aurora.AuroraSchedulerTest",
    "com.twitter.heron.scheduler.aurora.AuroraLauncherTest",
    "com.twitter.heron.scheduler.aurora.AuroraCLIControllerTest",
    "com.twitter.heron.scheduler.aurora.AuroraContextTest",
  ],
  runtime_deps = [ ":aurora-tests" ],
  size = "small",
)

java_library(
  name = "yarn-tests",
  srcs = glob(["**/yarn/*.java"]),
  deps = scheduler_deps_files + yarn_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.scheduler.yarn.YarnSchedulerTest",
    "com.twitter.heron.scheduler.yarn.HeronMasterDriverTest",
    "com.twitter.heron.scheduler.yarn.HeronExecutorTaskTest",
    "com.twitter.heron.scheduler.yarn.YarnLauncherTest",
  ],
  runtime_deps = [ ":yarn-tests" ],
  size = "small",
)

java_library(
  name = "local-tests",
  srcs = glob(["**/local/*.java"]),
  deps = scheduler_deps_files + local_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.scheduler.local.LocalLauncherTest",
    "com.twitter.heron.scheduler.local.LocalSchedulerTest",
  ],
  runtime_deps = [ ":local-tests" ],
  size = "small",
)

java_library(
  name = "slurm-tests",
  srcs = glob(["**/slurm/*.java"]),
  deps = scheduler_deps_files + slurm_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.scheduler.slurm.SlurmLauncherTest",
    "com.twitter.heron.scheduler.slurm.SlurmSchedulerTest",
    "com.twitter.heron.scheduler.slurm.SlurmControllerTest",
  ],
  runtime_deps = [ ":slurm-tests" ],
  size = "small",
)

java_library(
  name = "mesos-tests",
  srcs = glob(["**/mesos/**/*.java"]),
  deps = scheduler_deps_files + mesos_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.scheduler.mesos.MesosLauncherTest",
    "com.twitter.heron.scheduler.mesos.MesosSchedulerTest",
    "com.twitter.heron.scheduler.mesos.framework.TaskUtilsTest",
    "com.twitter.heron.scheduler.mesos.framework.TaskResourcesTest",
    "com.twitter.heron.scheduler.mesos.framework.LaunchableTaskTest",
    "com.twitter.heron.scheduler.mesos.framework.MesosFrameworkTest",
  ],
  runtime_deps = [ ":mesos-tests" ],
  size = "small",
)

java_library(
  name = "marathon-tests",
  srcs = glob(["**/marathon/*.java"]),
  deps = scheduler_deps_files + marathon_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.scheduler.marathon.MarathonSchedulerTest",
    "com.twitter.heron.scheduler.marathon.MarathonLauncherTest",
    "com.twitter.heron.scheduler.marathon.MarathonControllerTest",
  ],
  runtime_deps = [ ":marathon-tests" ],
  size = "small",
)
