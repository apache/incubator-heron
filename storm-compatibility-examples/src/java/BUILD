load("@rules_java//java:defs.bzl", "java_binary")

package(default_visibility = ["//visibility:public"])

java_binary(
    name = "heron-storm-compatibility-examples-unshaded",
    srcs = glob(["**/*.java"]),
    create_executable = 0,
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//storm-compatibility/src/java:storm-compatibility-java",
    ],
)

genrule(
    name = "heron-storm-compatibility-examples",
    srcs = [":heron-storm-compatibility-examples-unshaded_deploy.jar"],
    outs = ["heron-storm-compatibility-examples.jar"],
    cmd = "cp $< $@",
)
