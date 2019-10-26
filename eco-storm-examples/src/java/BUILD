package(default_visibility = ["//visibility:public"])

filegroup(
    name = "storm-eco-examples-support",
    srcs = glob(["**/*.yaml", "**/*.properties"]),
)

java_binary(
    name='storm-eco-examples-unshaded',
    srcs = glob(["org/apache/heron/examples/eco/**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:basics-java",
        "//storm-compatibility/src/java:storm-compatibility-java",
    ],
    create_executable = 0,
)

genrule(
    name = 'storm-eco-examples',
    srcs = [":storm-eco-examples-unshaded_deploy.jar"],
    outs = ["storm-eco-examples.jar"],
    cmd  = "cp $< $@",
)
