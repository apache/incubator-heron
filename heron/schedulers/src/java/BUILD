package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
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

java_binary(
    name='local-scheduler-unshaded',
    srcs = glob(["**/local/*.java"]),
    deps = local_deps_files,
)

jarjar_binary(
    name = "heron-local-scheduler",
    src = ":local-scheduler-unshaded_deploy.jar",
    shade = "shade.conf",
)
