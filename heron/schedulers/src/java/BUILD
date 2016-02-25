package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = heron_java_proto_files() + [
    "//heron/common/src/java:basics-java",
    "//heron/api/src/java:api-java",
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:scheduler-spi-java",
    "//heron/spi/src/java:utils-spi-java",
    "//3rdparty/guava:guava-java",
]


java_library(
    name='local-scheduler-java',
    srcs = glob(["**/local/*.java"]),
    deps = common_deps_files,
)
