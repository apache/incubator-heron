load("@rules_java//java:defs.bzl", "java_binary")

package(default_visibility = ["//visibility:public"])

java_binary(
    name = "api-examples-unshaded",
    srcs = glob(["org/apache/heron/examples/api/**/*.java"]),
    create_executable = 0,
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/simulator/src/java:simulator-java",
    ],
)

genrule(
    name = "heron-api-examples",
    srcs = [":api-examples-unshaded_deploy.jar"],
    outs = ["heron-api-examples.jar"],
    cmd = "cp $< $@",
)

java_binary(
    name = "streamlet-examples-unshaded",
    srcs = glob(["org/apache/heron/examples/streamlet/**/*.java"]),
    create_executable = 0,
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:basics-java",
        "//heron/simulator/src/java:simulator-java",
        "//third_party/java:kryo",
        "@maven//:org_apache_pulsar_pulsar_client_shaded",
    ],
)

genrule(
    name = "heron-streamlet-examples",
    srcs = [":streamlet-examples-unshaded_deploy.jar"],
    outs = ["heron-streamlet-examples.jar"],
    cmd = "cp $< $@",
)
