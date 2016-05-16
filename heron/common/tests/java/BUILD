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
    name = "sys_utils_unittest",
    srcs = glob(["**/SysUtilsTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "wakeable-looper_unittest",
    srcs = glob(["**/WakeableLooperTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "file-utility_unittest",
    srcs = glob(["**/FileUtilsTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//3rdparty/java:powermock",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "echo_unittest",
    srcs = glob(["**/EchoTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_networktests_java",
        "@protobuf-java//jar",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "heron-server_unittest",
    srcs = glob(["**/HeronServerTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_networktests_java",
        "@protobuf-java//jar",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_library(
    name = "config-test-java",
    srcs = glob(["**/config/Constants.java"]),
)

java_test(
    name = "config-reader_unittest",
    srcs = glob(["**/config/ConfigReaderTest.java"]),
    deps = [
        ":config-test-java",
        "//heron/common/src/java:config-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/config/testdata/**/*.yaml"]),
    size = "small",
)

java_test(
    name = "cluster-config-reader_unittest",
    srcs = glob(["**/config/ClusterConfigReaderTest.java"]),
    deps = [
        ":config-test-java",
        "//heron/common/src/java:config-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/config/testdata/**/*.yaml"]),
    size = "small",
)

java_test(
    name = "system-config_unittest",
    srcs = glob(["**/config/SystemConfigTest.java"]),
    deps = [
        ":config-test-java",
        "//heron/common/src/java:config-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/config/testdata/sysconfig.yaml"]),
    size = "small",
)
