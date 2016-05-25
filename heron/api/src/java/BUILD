licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

load("/tools/rules/heron_deps", "heron_java_proto_files_nl")

api_deps_files =  \
    heron_java_proto_files() + [
        "//heron/common/src/java:basics-java",
        "//3rdparty/java:kryo",
    ]

api_deps_files_nl =  \
    heron_java_proto_files_nl() + [
        "//heron/common/src/java:basics-java-nl",
        "//3rdparty/java:kryo-nl",
    ]

java_library(
    name = "api-java",
    srcs = glob(["**/*.java"]),
    deps = api_deps_files,
)

java_library(
    name = "api-java-nl",
    srcs = glob(["**/*.java"]),
    deps = api_deps_files_nl,
)

java_binary(
    name = "api-unshaded",
    srcs = glob(["**/*.java"]),
    deps = api_deps_files,
)

genrule(
    name = "heron-api",
    srcs = [":api-unshaded_deploy.jar"],
    outs = ["heron-api.jar"],
    cmd  = "cp $< $@",
)
