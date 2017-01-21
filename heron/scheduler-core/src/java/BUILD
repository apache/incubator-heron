package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
   "//heron/common/src/java:common-java",
   "//heron/api/src/java:classification",
   "@commons_cli_commons_cli//jar",
   "@com_google_guava_guava//jar",
]

spi_deps_files = [
    "//heron/api/src/java:api-java",
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

java_library(
    name = 'scheduler-java',
    srcs = glob(
        ["**/*.java"],
    ),
    deps = scheduler_deps_files,
)

java_binary(
    name = 'scheduler-unshaded',
    srcs = glob(["**/*.java"]),
    deps = scheduler_deps_files,
)

genrule(
    name = "heron-scheduler",
    srcs = [":scheduler-unshaded_deploy.jar"],
    outs = ["heron-scheduler.jar"],
    cmd  = "cp $< $@",
)
