load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//3rdparty/guava:guava-java",
    "//3rdparty/powermock:powermock-java",
    "//heron/api/src/java:api-java",
    "//heron/newscheduler/src/java:scheduler-java",
    "@bazel_tools//third_party:junit4",
    "@bazel_tools//third_party:mockito",
]

spi_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:uploader-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/spi/src/java:scheduler-spi-java",
    "//heron/spi/src/java:utils-spi-java",
]

scheduler_deps_files = \
    common_deps_files + \
    heron_java_proto_files() + \
    spi_deps_files

java_test(
    name = "launch-runner_unittest",
    srcs = glob(
        ["**/LaunchRunnerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = scheduler_deps_files,
    size = "small",
)

# java_test(
#     name = "scheduler-main_unittest",
#     srcs = glob(["**/SchedulerMainTest.java"]),
#     deps = scheduler_deps_files,
#     size = "small",
# )

# java_test(
#     name = "submitter-main_unittest",
#     srcs = glob(["**/SubmitterMainTest.java"]),
#     deps = scheduler_deps_files,
#     size = "small",
# )

