load("@rules_java//java:defs.bzl", "java_library")

common_deps_files = [
    "@maven//:com_google_guava_guava",
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
    "@maven//:org_apache_reef_reef_common",
    "@maven//:org_apache_reef_tang",
    "@maven//:org_apache_reef_wake",
    "@maven//:org_apache_reef_reef_runtime_local",
    "@maven//:org_apache_reef_reef_runtime_yarn",
    "@maven//:org_glassfish_hk2_external_javax_inject",
    "@maven//:org_apache_avro_avro",
    "@maven//:commons_lang_commons_lang",
    "@maven//:io_netty_netty_all",
    "@maven//:org_codehaus_jackson_jackson_core_asl",
    "@maven//:org_codehaus_jackson_jackson_mapper_asl",
]

local_deps_files = [
    "//heron/schedulers/src/java:local-scheduler-java",
]

mesos_deps_files = [
    "//heron/schedulers/src/java:mesos-scheduler-java",
    "@maven//:org_apache_mesos_mesos",
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
    "@maven//:org_slf4j_slf4j_api",
]

nomad_sdk_deps = [
    "@maven//:com_hashicorp_nomad_nomad_sdk",
    "@maven//:com_fasterxml_jackson_core_jackson_annotations",
    "@maven//:com_fasterxml_jackson_core_jackson_core",
    "@maven//:com_fasterxml_jackson_core_jackson_databind",
    "@maven//:com_google_code_findbugs_jsr305",
    "@maven//:commons_codec_commons_codec",
    "@maven//:commons_logging_commons_logging",
    "@maven//:org_apache_httpcomponents_httpclient",
    "@maven//:org_apache_httpcomponents_httpcore",
    "@maven//:org_bouncycastle_bcpkix_jdk15on",
    "@maven//:org_bouncycastle_bcprov_jdk15on",
]

nomad_deps_files = \
    nomad_sdk_deps + [
        "//heron/schedulers/src/java:nomad-scheduler-java",
        "//heron/schedulers/src/java:scheduler-utils-java",
    ]

java_library(
    name = "aurora-tests",
    srcs = glob(["**/aurora/*.java"]),
    deps = scheduler_deps_files + aurora_deps_files + ["@maven//:commons_cli_commons_cli"],
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.aurora.AuroraSchedulerTest",
        "org.apache.heron.scheduler.aurora.AuroraLauncherTest",
        "org.apache.heron.scheduler.aurora.AuroraCLIControllerTest",
        "org.apache.heron.scheduler.aurora.AuroraContextTest",
    ],
    runtime_deps = [":aurora-tests"],
)

java_library(
    name = "yarn-tests",
    srcs = glob(["**/yarn/*.java"]),
    deps = scheduler_deps_files + yarn_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.yarn.YarnSchedulerTest",
        "org.apache.heron.scheduler.yarn.HeronMasterDriverTest",
        "org.apache.heron.scheduler.yarn.HeronExecutorTaskTest",
        "org.apache.heron.scheduler.yarn.YarnLauncherTest",
    ],
    runtime_deps = [":yarn-tests"],
)

java_library(
    name = "local-tests",
    srcs = glob(["**/local/*.java"]),
    deps = scheduler_deps_files + local_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.local.LocalLauncherTest",
        "org.apache.heron.scheduler.local.LocalSchedulerTest",
    ],
    runtime_deps = [":local-tests"],
)

java_library(
    name = "slurm-tests",
    srcs = glob(["**/slurm/*.java"]),
    deps = scheduler_deps_files + slurm_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.slurm.SlurmLauncherTest",
        "org.apache.heron.scheduler.slurm.SlurmSchedulerTest",
        "org.apache.heron.scheduler.slurm.SlurmControllerTest",
    ],
    runtime_deps = [":slurm-tests"],
)

java_library(
    name = "mesos-tests",
    srcs = glob(["**/mesos/**/*.java"]),
    deps = scheduler_deps_files + mesos_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.mesos.MesosLauncherTest",
        "org.apache.heron.scheduler.mesos.MesosSchedulerTest",
        "org.apache.heron.scheduler.mesos.framework.TaskUtilsTest",
        "org.apache.heron.scheduler.mesos.framework.TaskResourcesTest",
        "org.apache.heron.scheduler.mesos.framework.LaunchableTaskTest",
        "org.apache.heron.scheduler.mesos.framework.MesosFrameworkTest",
    ],
    runtime_deps = [":mesos-tests"],
)

java_library(
    name = "marathon-tests",
    srcs = glob(["**/marathon/*.java"]),
    deps = scheduler_deps_files + marathon_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.marathon.MarathonSchedulerTest",
        "org.apache.heron.scheduler.marathon.MarathonLauncherTest",
        "org.apache.heron.scheduler.marathon.MarathonControllerTest",
    ],
    runtime_deps = [":marathon-tests"],
)

java_library(
    name = "kubernetes-tests",
    srcs = glob(["**/kubernetes/*.java"]),
    deps = scheduler_deps_files + kubernetes_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.kubernetes.KubernetesSchedulerTest",
        "org.apache.heron.scheduler.kubernetes.KubernetesControllerTest",
        "org.apache.heron.scheduler.kubernetes.KubernetesLauncherTest",
        "org.apache.heron.scheduler.kubernetes.VolumesTests",
    ],
    runtime_deps = [":kubernetes-tests"],
)

java_library(
    name = "nomad-tests",
    srcs = glob(["**/nomad/*.java"]),
    deps = scheduler_deps_files + nomad_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.scheduler.nomad.NomadSchedulerTest",
    ],
    runtime_deps = [":nomad-tests"],
)
