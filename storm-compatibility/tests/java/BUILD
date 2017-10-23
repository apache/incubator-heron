package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

java_library(
    name = "storm-api-tests",
    srcs = glob(["**/*.java"]),
    deps =  [
        "//storm-compatibility/src/java:storm-compatibility-java",
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:basics-java",
        "//third_party/java:junit4",
    ],
)

java_tests(
    test_classes = [
        "org.apache.storm.SimulatorIntegrationTest",
    ],
    runtime_deps = [
        ":storm-api-tests",
    ],
    size = "small",
)
