licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

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
        "@javax_xml_bind_jaxb//jar",
        "@org_yaml_snakeyaml//jar",
    ]

java_library(
    name = "simulator-java",
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    srcs = glob(["**/*.java"]),
    deps = simulator_deps_files,
)

java_binary(
    name = "simulator-unshaded",
    srcs = glob(["org/apache/heron/simulator/**/*.java"]),
    deps = simulator_deps_files + [
        "//third_party/java:kryo-neverlink",
        "@org_apache_commons_commons_lang3//jar"
    ]
)

jarjar_binary(
    name = "simulator-shaded",
    src = ":simulator-unshaded_deploy.jar",
    shade = "shade.conf",
    deps = [
        "@org_sonatype_plugins_jarjar_maven_plugin//jar"
    ]
)

genrule(
    name = "heron-simulator",
    srcs = [":simulator-shaded"],
    outs = ["heron-simulator.jar"],
    cmd = "cp $< $@",
)