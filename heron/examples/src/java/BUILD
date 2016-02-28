package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

java_binary(
    name='examples-unshaded',
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
    ],
    create_executable = 0,
)

jarjar_binary(
    name = 'heron-examples',
    src  = ":examples-unshaded_deploy.jar",
    shade = "shade.conf",
)
