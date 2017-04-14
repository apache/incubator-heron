licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

api_deps_files =  \
    heron_java_proto_files() + [
        ":classification",
        "//heron/common/src/java:basics-java",
        "//third_party/java:kryo",
    ]

java_library(
    name = "api-java",
    srcs = glob(["com/twitter/heron/api/**/*.java"]),
    deps = api_deps_files,
)

alias(
    name = "heron-api",
    actual = ":api-java",
)

java_library(
    name = "classification",
    srcs = glob(["com/twitter/heron/classification/**/*.java"]),
    resources = ["//heron/api/src:processor-service-conf"],
)
