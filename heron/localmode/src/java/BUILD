licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron", "heron_java_proto_files")

java_library(
    name = "localmode-lib-java",
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//3rdparty/snakeyaml:snakeyaml-java",
    ] + heron_java_proto_files(),
)
