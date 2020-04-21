load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_doc(
    name = "heron-api-javadoc",
    libs = [":api-java-low-level-functional"],
    pkgs = ["org/apache/heron"],
    title = "Heron Api Documentation",
)

api_deps_files = \
    heron_java_api_proto_files() + [
        ":classification",
        "//heron/common/src/java:basics-java",
        "@maven//:javax_xml_bind_jaxb_api",
    ]

# Low Level Api
java_library(
    name = "api-java-low-level",
    srcs = glob(["org/apache/heron/api/**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = api_deps_files + [
        "//third_party/java:kryo-neverlink",
    ],
)

# Functional Api
java_library(
    name = "api-java",
    srcs = glob(["org/apache/heron/streamlet/**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = api_deps_files + [
        ":api-java-low-level",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

# Low level and functional Api
java_library(
    name = "api-java-low-level-functional",
    srcs = glob([
        "org/apache/heron/api/**/*.java",
        "org/apache/heron/streamlet/**/*.java",
    ]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = api_deps_files + [
        "//third_party/java:kryo-neverlink",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_binary(
    name = "api-unshaded",
    srcs = glob([
        "org/apache/heron/api/**/*.java",
        "org/apache/heron/streamlet/**/*.java",
    ]),
    deps = api_deps_files + [
        "//third_party/java:kryo-neverlink",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

jarjar_binary(
    name = "api-shaded",
    src = ":api-unshaded_deploy.jar",
    shade = "shade.conf",
    deps = ["@maven//:org_sonatype_plugins_jarjar_maven_plugin"],
)

genrule(
    name = "heron-api",
    srcs = [":api-shaded"],
    outs = ["heron-api.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "classification",
    srcs = glob(["org/apache/heron/classification/**/*.java"]),
    resources = ["//heron/api/src:processor-service-conf"],
)
