licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

scala_doc(
    name = "heron-api-scaladoc",
    libs = [":api-scala"],
    pkgs = ["com/twitter/heron"],
    title = "Heron Scala Streamlet Documentation",
)

# Low Level Api
scala_library(
    name = "api-scala-low-level",
    srcs = glob(["com/twitter/heron/streamlet/*.scala"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = api_deps_files,
)

# Functional Api
java_library(
    name = "api-java",
    srcs = glob(["com/twitter/heron/streamlet/impl/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = api_deps_files + [
        ":api-java-low-level",
        "//third_party/java:kryo-neverlink",
    ],
)

scala_binary(
    name = "api-unshaded",
    srcs = glob([
        "com/twitter/heron/streamlet",
        "com/twitter/heron/streamlet/impl/*.java",
    ]),
)

jarjar_binary(
    name = "streamlet-scala-shaded",
    src = ":api-unshaded_deploy.jar",
    shade = "shade.conf",
    deps = ["@org_sonatype_plugins_jarjar_maven_plugin//jar"],
)

genrule(
    name = "heron-scala-streamlet",
    srcs = [":streamlet-scala-shaded"],
    outs = ["heron-scala-streamlet.jar"],
    cmd = "cp $< $@",
)
