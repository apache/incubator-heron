
load("/tools/rules/heron_deps", "heron_java_proto_files")

java_test(
    name = "aurora-config-loader_unittest",
    srcs = glob(["**/aurora/AuroraConfigLoaderTest.java"]),
    deps = [
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "aurora-launcher_unittest",
    srcs = glob(
        ["**/aurora/AuroraLauncherTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "packer-uploader_unittest",
    srcs = glob(
        ["**/twitter/PackerUploaderTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "aurora-topology-runtime_unittest",
    srcs = glob(
        ["**/aurora/AuroraTopologyRuntimeManagerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "local-launcher_unittest",
    srcs = glob(
        ["**/local/LocalLauncherTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/common/src/java:basics-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-local-scheduler-lib",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "local-uploader_unittest",
    srcs = glob(
        ["**/local/LocalUploaderTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/common/src/java:basics-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-local-scheduler-lib",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "local-scheduler_unittest",
    srcs = glob(
        ["**/local/LocalSchedulerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-local-scheduler-lib",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "health-check-runner_unittest",
    srcs = glob(["**/service/HealthCheckRunnerTest.java"]),
    deps = [
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "launch-runner_unittest",
    srcs = glob(
        ["**/service/LaunchRunnerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/guava:guava-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "scheduler-main_unittest",
    srcs = glob(["**/service/SchedulerMainTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "submitter-main_unittest",
    srcs = glob(["**/service/SubmitterMainTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "upload-runner_unittest",
    srcs = glob(
        ["**/service/UploadRunnerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "scheduler-server_unittest",
    srcs = glob(
        ["**/service/server/SchedulerServerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "base64-config-loader_unittest",
    srcs = glob(["**/util/Base64ConfigLoaderTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "default-config-loader_unittest",
    srcs = glob(["**/util/DefaultConfigLoaderTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "network-utility_unittest",
    srcs = glob(["**/util/NetworkUtilityTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "round-robin-packing_unittest",
    srcs = glob(
        ["**/util/RoundRobinPackingTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "shell-utility_unittest",
    srcs = glob(["**/util/ShellUtilityTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "topology-utility_unittest",
    srcs = glob(["**/util/TopologyUtilityTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),

    size = "small",
)
