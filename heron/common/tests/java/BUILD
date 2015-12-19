java_test(
    name = "communicator_unittest",
    srcs = glob(["**/CommunicatorTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "wakeable_looper_unittest",
    srcs = glob(["**/WakeableLooperTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "file_utility_unittest",
    srcs = glob(["**/FileUtilityTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "echo_unittest",
    srcs = glob(["**/EchoTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_networktests_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "heron_server_unittest",
    srcs = glob(["**/HeronServerTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_networktests_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)
