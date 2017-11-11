package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:utils-java",
    "@commons_io_commons_io//jar",
    "@com_google_guava_guava//jar",
]

spi_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:scheduler-spi-java",
    "//heron/spi/src/java:utils-spi-java",
]

api_deps_files = [
    "//heron/api/src/java:api-java-low-level",
]

scheduler_deps_files = \
    ["//heron/scheduler-core/src/java:scheduler-java"] + \
    heron_java_proto_files() + \
    common_deps_files + \
    spi_deps_files + \
    api_deps_files

yarn_deps_files = \
    scheduler_deps_files + [
        "//third_party/java:yarn",
    ]

mesos_deps_files = \
    scheduler_deps_files + [
       "//third_party/java:jackson",
       "@org_apache_mesos_mesos//jar",
       "@org_apache_curator_curator_client//jar",
       "@org_apache_curator_curator_framework//jar",
       "@org_apache_zookeeper_zookeeper//jar",
   ]

marathon_deps_files = \
    scheduler_deps_files + [
       "//third_party/java:jackson",
    ]

kubernetes_deps_files = \
    scheduler_deps_files + [
        ":scheduler-utils-java",
        "//third_party/java:jackson",
    ]

ecs_deps_files = \
    scheduler_deps_files + [
        ":scheduler-utils-java",
        "//third_party/java:jackson",
    ]

java_library(
    name='local-scheduler-java',
    srcs = glob(["**/local/*.java"]),
    deps = scheduler_deps_files,
)

java_binary(
    name='local-scheduler-unshaded',
    srcs = glob(["**/local/*.java"]),
    deps = scheduler_deps_files,
)

genrule(
    name = "heron-local-scheduler",
    srcs = [":local-scheduler-unshaded_deploy.jar"],
    outs = ["heron-local-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='aurora-scheduler-java',
    srcs = glob(["**/aurora/*.java"]),
    deps = scheduler_deps_files,
    resources = glob(["**/aurora/*.aurora"]),
)

java_binary(
    name='aurora-scheduler-unshaded',
    srcs = glob(["**/aurora/*.java"]),
    deps = scheduler_deps_files,
    resources = glob(["**/aurora/*.aurora"]),
)

genrule(
    name = "heron-aurora-scheduler",
    srcs = [":aurora-scheduler-unshaded_deploy.jar"],
    outs = ["heron-aurora-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='null-scheduler-java',
    srcs = glob(
        ["**/NullLauncher.java"] +
        ["**/NullScheduler.java"]
    ),
    deps = scheduler_deps_files,
)

java_binary(
    name='null-scheduler-unshaded',
    srcs = glob(
        ["**/NullLauncher.java"] +
        ["**/NullScheduler.java"]
    ),
    deps = scheduler_deps_files,
)

genrule(
    name = "heron-null-scheduler",
    srcs = [":null-scheduler-unshaded_deploy.jar"],
    outs = ["heron-null-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='slurm-scheduler-java',
    srcs = glob(["**/slurm/*.java"]),
    deps = scheduler_deps_files,
)

java_binary(
    name='slurm-scheduler-unshaded',
    srcs = glob(["**/slurm/*.java"]),
    deps = scheduler_deps_files,
)

genrule(
    name = "heron-slurm-scheduler",
    srcs = [":slurm-scheduler-unshaded_deploy.jar"],
    outs = ["heron-slurm-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='yarn-scheduler-java',
    srcs = glob(["**/yarn/*.java"]),
    deps = yarn_deps_files,
)

java_binary(
    name='yarn-scheduler-unshaded',
    srcs = glob(["**/yarn/*.java"]),
    deps = yarn_deps_files,
)

genrule(
    name = "heron-yarn-scheduler",
    srcs = [":yarn-scheduler-unshaded_deploy.jar"],
    outs = ["heron-yarn-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='mesos-scheduler-java',
    srcs = glob(["com/twitter/heron/scheduler/mesos/**/*.java"]),
    deps = mesos_deps_files,
)

java_binary(
    name='mesos-scheduler-unshaded',
    srcs = glob(["com/twitter/heron/scheduler/mesos/**/*.java"]),
    deps = mesos_deps_files,
)

genrule(
    name = "heron-mesos-scheduler",
    srcs = [":mesos-scheduler-unshaded_deploy.jar"],
    outs = ["heron-mesos-scheduler.jar"],
    cmd  = "cp $< $@",
)


java_library(
    name='marathon-scheduler-java',
    srcs = glob(["com/twitter/heron/scheduler/marathon/**/*.java"]),
    deps = marathon_deps_files,
)

java_binary(
    name='marathon-scheduler-unshaded',
    srcs=glob(["com/twitter/heron/scheduler/marathon/**/*.java"]),
    deps = marathon_deps_files,
)

genrule(
    name = "heron-marathon-scheduler",
    srcs = [":marathon-scheduler-unshaded_deploy.jar"],
    outs = ["heron-marathon-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='kubernetes-scheduler-java',
    srcs = glob(["com/twitter/heron/scheduler/kubernetes/**/*.java"]),
    deps = kubernetes_deps_files,
)

java_binary(
    name='kubernetes-scheduler-unshaded',
    srcs=glob(["com/twitter/heron/scheduler/kubernetes/**/*.java"]),
    deps = kubernetes_deps_files,
)

genrule(
    name = "heron-kubernetes-scheduler",
    srcs = [":kubernetes-scheduler-unshaded_deploy.jar"],
    outs = ["heron-kubernetes-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='ecs-scheduler-java',
    srcs = glob(["com/twitter/heron/scheduler/ecs/**/*.java"]),
    deps = ecs_deps_files,
)

java_binary(
    name='ecs-scheduler-unshaded',
    srcs=glob(["com/twitter/heron/scheduler/ecs/**/*.java"]),
    deps = ecs_deps_files,
)

genrule(
    name = "heron-ecs-scheduler",
    srcs = [":ecs-scheduler-unshaded_deploy.jar"],
    outs = ["heron-ecs-scheduler.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name = 'scheduler-utils-java',
    srcs = glob(["com/twitter/heron/scheduler/utils/**/*.java"]),
    deps = [
        "//third_party/java:jackson",
        "//heron/spi/src/java:network-utils-java",
    ]
)
