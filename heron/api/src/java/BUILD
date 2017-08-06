licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")
load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/build_defs", "DOCLINT_HTML_AND_SYNTAX")
load("/tools/rules/javadoc", "java_doc")

java_doc(
    name = "heron-api-javadoc",
    libs = [":api-java"],
    pkgs = ["com/twitter/heron"],
    title = "Heron Api Documentation",
)

api_deps_files =  \
    heron_java_proto_files() + [
        ":classification",
        "//heron/common/src/java:basics-java",
    ]

java_library(
    name = "api-java",
    srcs = glob(["com/twitter/heron/api/**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = api_deps_files,
)

java_binary(
    name = "api",
    srcs = glob(["com/twitter/heron/api/**/*.java"]),
    deps = api_deps_files,
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
