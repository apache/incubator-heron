java_test(
    name = "local_file_state_manager_unittest",
    srcs = glob(
        ["**/localfile/LocalFileStateManagerTest.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:base-java",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)
