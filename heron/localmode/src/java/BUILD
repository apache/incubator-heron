licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

localmode_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//3rdparty/snakeyaml:snakeyaml-java",
    ]

java_library(
    name = "localmode-java",
    srcs = glob(["**/*.java"]),
    deps = localmode_deps_files, 
)
