package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
   "//heron/common/src/java:common-java",
   "//3rdparty/commons:commons-cli-java",
   "//3rdparty/guava:guava-java",
]

spi_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:utils-spi-java",
]

commands_deps_files = \
    common_deps_files + \
    heron_java_proto_files() + \
    spi_deps_files

java_library(
    name = 'commands-java',
    srcs = glob(
        ["**/*.java"],
    ),
    deps = commands_deps_files,
)

java_binary(
    name = 'commands-unshaded',
    srcs = glob(["**/*.java"]),
    deps = commands_deps_files,
)

genrule(
    name = "heron-commands",
    srcs = [":commands-unshaded_deploy.jar"],
    outs = ["heron-commands.jar"],
    cmd  = "cp $< $@",
)
