package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

java_library(
    name = 'newscheduler-java',
    srcs = glob([
        "**/*.java",
    ]),
    deps = [
        "//heron/common/src/java:config-java",
        "//heron/spi/src/java:common-spi-java",
        "//heron/spi/src/java:uploader-spi-java",
        "//heron/spi/src/java:packing-spi-java",
    ], 
)
