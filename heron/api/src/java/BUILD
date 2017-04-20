licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")
load("/tools/rules/jarjar_rules", "jarjar_binary")

api_deps_files =  \
    heron_java_proto_files() + [
        ":classification",
        "//heron/common/src/java:basics-java",
        "//third_party/java:kryo",
    ]

# Kryo dependency should be specified in POM
api_deps_files_nl =  \
    heron_java_proto_files() + [
        ":classification",
        "//heron/common/src/java:basics-java",
        "//third_party/java:kryo-neverlink",
    ]

java_library(
    name = "api-java",
    srcs = glob(["com/twitter/heron/api/**/*.java"]),
    deps = api_deps_files,
)

java_library(
    name = "api-java-neverlink",
    srcs = glob(["com/twitter/heron/api/**/*.java"]),
    deps = api_deps_files_nl,
)

java_binary(
    name = "api",
    srcs = glob(["com/twitter/heron/api/**/*.java"]),
    deps = api_deps_files_nl,
)

jarjar_binary(
    name = "heron-api",
    src = ":api_deploy.jar",
    shade = "shade.conf",
    deps = ["@org_sonatype_plugins_jarjar_maven_plugin//jar"]
)

java_library(
    name = "classification",
    srcs = glob(["com/twitter/heron/classification/**/*.java"]),
    resources = ["//heron/api/src:processor-service-conf"],
)
