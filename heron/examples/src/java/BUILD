package(default_visibility = ["//visibility:public"])

java_binary(
    name='examples-unshaded',
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/storm/src/java:storm-compatibility-java",
    ],
    create_executable = 0,
)

genrule(
    name = 'heron-examples',
    srcs = [":examples-unshaded_deploy.jar"],
    outs = ["heron-examples.jar"],
    cmd  = "cp $< $@",
)
