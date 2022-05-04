load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_doc(
    name = "heron-simulator-javadoc",
    libs = [":simulator-java"],
    pkgs = ["org/apache/heron"],
    title = "Heron Simulator Documentation",
)

simulator_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java-low-level-functional",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:utils-java",
        "//heron/instance/src/java:instance-java",
        "@maven//:org_yaml_snakeyaml",
        "@maven//:javax_xml_bind_jaxb_api",
    ]

java_library(
    name = "simulator-java",
    srcs = glob(["**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = simulator_deps_files,
)

java_binary(
    name = "simulator-unshaded",
    srcs = glob(["org/apache/heron/simulator/**/*.java"]),
    resources = ["//:disclaimers"],
    deps = simulator_deps_files + [
        "//third_party/java:kryo-neverlink",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

jar_jar(
    name = "simulator-shaded",
    input_jar = ":simulator-unshaded_deploy.jar",
    rules = "shade.conf",
)

genrule(
    name = "heron-simulator",
    srcs = [":simulator-shaded"],
    outs = ["heron-simulator.jar"],
    cmd = "cp $< $@",
)
