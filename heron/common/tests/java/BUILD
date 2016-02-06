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

java_library(
    name = "config_test_library",
    srcs = glob(["**/config/Constants.java"]),
)

java_test(
    name = "config_reader_unittest",
    srcs = glob(["**/config/ConfigReaderTest.java"]),
    deps = [
        ":config_test_library",
        "//heron/common/src/java:config-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/config/testdata/**/*.yaml"]),
    size = "small",
)

java_test(
    name = "heron_config_reader_unittest",
    srcs = glob(["**/config/HeronConfigReaderTest.java"]),
    deps = [
        ":config_test_library",
        "//heron/common/src/java:config-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/config/testdata/**/*.yaml"]),
    size = "small",
)

