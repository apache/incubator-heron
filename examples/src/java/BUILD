package(default_visibility = ["//visibility:public"])

java_binary(
    name='api-examples-unshaded',
    srcs = glob(["com/twitter/heron/examples/api/**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:basics-java",
        "//heron/simulator/src/java:simulator-java"
    ],
    create_executable = 0,
)

genrule(
    name = 'heron-api-examples',
    srcs = [":api-examples-unshaded_deploy.jar"],
    outs = ["heron-api-examples.jar"],
    cmd  = "cp $< $@",
)

java_binary(
    name='dsl-examples-unshaded',
    srcs = glob(["com/twitter/heron/examples/dsl/**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:basics-java",
        "//heron/simulator/src/java:simulator-java"
    ],
    create_executable = 0,
)

genrule(
    name = 'heron-dsl-examples',
    srcs = [":dsl-examples-unshaded_deploy.jar"],
    outs = ["heron-dsl-examples.jar"],
    cmd  = "cp $< $@",
)
