java_test(
    name = "aurora_config_loader_unittest",
    srcs = glob(["**/aurora/AuroraConfigLoaderTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "aurora_launcher_unittest",
    srcs = glob(
        ["**/aurora/AuroraLauncherTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "packer_uploader_unittest",
    srcs = glob(
        ["**/twitter/PackerUploaderTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/state/src/java:state-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "aurora_topology_runtime_unittest",
    srcs = glob(
        ["**/aurora/AuroraTopologyRuntimeManagerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:scheduler-twitter-lib-java",
        "//heron/scheduler/src/java:heron-aurora-scheduler-lib",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "local_launcher_unittest",
    srcs = glob(
        ["**/local/LocalLauncherTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-local-scheduler-lib",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "local_uploader_unittest",
    srcs = glob(
        ["**/local/LocalUploaderTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-local-scheduler-lib",
        "//heron/state/src/java:state-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "local_scheduler_unittest",
    srcs = glob(
        ["**/local/LocalSchedulerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/scheduler/src/java:heron-local-scheduler-lib",
        "//heron/state/src/java:state-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "health_check_runner_unittest",
    srcs = glob(["**/service/HealthCheckRunnerTest.java"]),
    deps = [
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "launch_runner_unittest",
    srcs = glob(
        ["**/service/LaunchRunnerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto_execution_state_java",
        "//3rdparty/guava:guava-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "scheduler_main_unittest",
    srcs = glob(["**/service/SchedulerMainTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/state/src/java:state-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "submitter_main_unittest",
    srcs = glob(["**/service/SubmitterMainTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto_execution_state_java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "upload_runner_unittest",
    srcs = glob(
        ["**/service/UploadRunnerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "scheduler_server_unittest",
    srcs = glob(
        ["**/service/server/SchedulerServerTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/state/src/java:state-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "base64_config_loader_unittest",
    srcs = glob(["**/util/Base64ConfigLoaderTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "default_config_loader_unittest",
    srcs = glob(["**/util/DefaultConfigLoaderTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "network_utility_unittest",
    srcs = glob(["**/util/NetworkUtilityTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/proto:proto_common_java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "round_robin_packing_unittest",
    srcs = glob(
        ["**/util/RoundRobinPackingTest.java"] +
        ["**/util/TopologyUtilityTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/state/src/java:state-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "shell_utility_unittest",
    srcs = glob(["**/util/ShellUtilityTest.java"]),
    deps = [
        "//heron/scheduler/src/java:scheduler-lib-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "topology_utility_unittest",
    srcs = glob(["**/util/TopologyUtilityTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/scheduler/src/java:scheduler-lib-java",
        "//heron/proto:proto-java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)
