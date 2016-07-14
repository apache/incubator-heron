java_test(
    name = "CommunicatorTest",
    srcs = glob(["**/CommunicatorTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "SysUtilsTest",
    srcs = glob(["**/SysUtilsTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "WakeableLooperTest",
    srcs = glob(["**/WakeableLooperTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "FileUtilsTest",
    srcs = glob(["**/FileUtilsTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//third_party/java:powermock",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "EchoTest",
    srcs = glob(["**/EchoTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_networktests_java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "HeronServerTest",
    srcs = glob(["**/HeronServerTest.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_networktests_java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_library(
    name = "config-test-java",
    srcs = glob(["**/config/Constants.java"]),
)

java_test(
    name = "ConfigReaderTest",
    srcs = glob(["**/config/ConfigReaderTest.java"]),
    deps = [
        ":config-test-java",
        "//heron/common/src/java:config-java",
        "//third_party/java:junit4",
    ],
    data = glob(["**/config/testdata/**/*.yaml"]),
    size = "small",
)

java_test(
    name = "ClusterConfigReaderTest",
    srcs = glob(["**/config/ClusterConfigReaderTest.java"]),
    deps = [
        ":config-test-java",
        "//heron/common/src/java:config-java",
        "//third_party/java:junit4",
    ],
    data = glob(["**/config/testdata/**/*.yaml"]),
    size = "small",
)

java_test(
    name = "SystemConfigTest",
    srcs = glob(["**/config/SystemConfigTest.java"]),
    deps = [
        ":config-test-java",
        "//heron/common/src/java:config-java",
        "//third_party/java:junit4",
    ],
    data = glob(["**/config/testdata/sysconfig.yaml"]),
    size = "small",
)
