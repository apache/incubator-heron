load("@rules_java//java:defs.bzl", "java_binary")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "heron-eco-examples-support",
    srcs = glob(["**/*.yaml"]),
)

java_binary(
    name = "eco-examples-unshaded",
    srcs = glob(["org/apache/heron/examples/eco/**/*.java"]),
    create_executable = 0,
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//storm-compatibility/src/java:storm-compatibility-java",
    ],
)

genrule(
    name = "heron-eco-examples",
    srcs = [":eco-examples-unshaded_deploy.jar"],
    outs = ["heron-eco-examples.jar"],
    cmd = "cp $< $@",
)
