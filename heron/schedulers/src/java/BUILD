package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//3rdparty/commons:commons-io-java",
    "//3rdparty/guava:guava-java",
]

spi_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:scheduler-spi-java",
    "//heron/spi/src/java:utils-spi-java",
]

api_deps_files = [
    "//heron/api/src/java:api-java",
]

local_deps_files = \
    heron_java_proto_files() + \
    common_deps_files + \
    spi_deps_files + \
    api_deps_files

java_library(
    name='local-scheduler-java',
    srcs = glob(["**/local/*.java"]),
    deps = local_deps_files,
    resources = glob(["**/local/*.yaml"]),
)

java_binary(
    name='local-scheduler-unshaded',
    srcs = glob(["**/local/*.java"]),
    deps = local_deps_files,
    resources = glob(["**/local/*.yaml"]),
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
    deps = local_deps_files,
    resources = glob(["**/aurora/*.aurora"]),
)

java_binary(
    name='aurora-scheduler-unshaded',
    srcs = glob(["**/aurora/*.java"]),
    deps = local_deps_files,
    resources = glob(["**/aurora/*.aurora"]),
)

genrule(
    name = "heron-aurora-scheduler",
    srcs = [":aurora-scheduler-unshaded_deploy.jar"],
    outs = ["heron-aurora-scheduler.jar"],
    cmd  = "cp $< $@",
)
