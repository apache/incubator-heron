common_deps_files = [
    "@com_google_guava_guava//jar",
    "//third_party/java:powermock",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:utils-java",
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

kubernetes_deps_files = [
    "//heron/schedulers/src/java:kubernetes-scheduler-java",
    "//heron/schedulers/src/java:scheduler-utils-java",
    "//third_party/java:kubernetes-java-client",
    "@org_slf4j_slf4j_api//jar",
]

nomad_sdk_deps = [
    "@com_hashicorp_nomad//jar",
    "@com_fasterxml_jackson_core_jackson_annotations//jar",
    "@com_fasterxml_jackson_core_jackson_core//jar",
    "@com_fasterxml_jackson_core_jackson_databind//jar",
    "@com_google_code_findbugs_jsr305//jar",
    "@commons_codec_commons_codec//jar",
    "@commons_logging_commons_logging//jar",
    "@org_apache_httpcomponents_http_client//jar",
    "@org_apache_httpcomponents_http_core//jar",
    "@org_bouncycastle_bcpkix_jdk15on//jar",
    "@org_bouncycastle_bcprov_jdk15on//jar",
]
    
nomad_deps_files = \
    nomad_sdk_deps +[
        "//heron/schedulers/src/java:nomad-scheduler-java",
        "//heron/schedulers/src/java:scheduler-utils-java",
    ]

java_library(
  name = "aurora-tests",
  srcs = glob(["**/aurora/*.java"]),
  deps = scheduler_deps_files + aurora_deps_files + ["@commons_cli_commons_cli//jar"],
)

java_tests(
  test_classes = [
    "org.apache.heron.scheduler.aurora.AuroraSchedulerTest",
    "org.apache.heron.scheduler.aurora.AuroraLauncherTest",
    "org.apache.heron.scheduler.aurora.AuroraCLIControllerTest",
    "org.apache.heron.scheduler.aurora.AuroraContextTest",
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
    "org.apache.heron.scheduler.yarn.YarnSchedulerTest",
    "org.apache.heron.scheduler.yarn.HeronMasterDriverTest",
    "org.apache.heron.scheduler.yarn.HeronExecutorTaskTest",
    "org.apache.heron.scheduler.yarn.YarnLauncherTest",
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
    "org.apache.heron.scheduler.local.LocalLauncherTest",
    "org.apache.heron.scheduler.local.LocalSchedulerTest",
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
    "org.apache.heron.scheduler.slurm.SlurmLauncherTest",
    "org.apache.heron.scheduler.slurm.SlurmSchedulerTest",
    "org.apache.heron.scheduler.slurm.SlurmControllerTest",
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
    "org.apache.heron.scheduler.mesos.MesosLauncherTest",
    "org.apache.heron.scheduler.mesos.MesosSchedulerTest",
    "org.apache.heron.scheduler.mesos.framework.TaskUtilsTest",
    "org.apache.heron.scheduler.mesos.framework.TaskResourcesTest",
    "org.apache.heron.scheduler.mesos.framework.LaunchableTaskTest",
    "org.apache.heron.scheduler.mesos.framework.MesosFrameworkTest",
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
    "org.apache.heron.scheduler.marathon.MarathonSchedulerTest",
    "org.apache.heron.scheduler.marathon.MarathonLauncherTest",
    "org.apache.heron.scheduler.marathon.MarathonControllerTest",
  ],
  runtime_deps = [ ":marathon-tests" ],
  size = "small",
)

java_library(
  name = "kubernetes-tests",
  srcs = glob(["**/kubernetes/*.java"]),
  deps = scheduler_deps_files + kubernetes_deps_files,
)

java_tests(
  test_classes = [
    "org.apache.heron.scheduler.kubernetes.KubernetesSchedulerTest",
    "org.apache.heron.scheduler.kubernetes.KubernetesControllerTest",
    "org.apache.heron.scheduler.kubernetes.KubernetesLauncherTest",
    "org.apache.heron.scheduler.kubernetes.VolumesTests",
  ],
  runtime_deps = [ ":kubernetes-tests" ],
  size = "small",
)

java_library(
  name = "nomad-tests",
  srcs = glob(["**/nomad/*.java"]),
  deps = scheduler_deps_files + nomad_deps_files,
)

java_tests(
  test_classes = [
    "org.apache.heron.scheduler.nomad.NomadSchedulerTest",
  ],
  runtime_deps = [ ":nomad-tests" ],
  size = "small",
)
