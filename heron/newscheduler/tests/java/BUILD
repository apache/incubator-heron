java_library(
    name = "test-utils-java",
    srcs = glob([
        "**/TestConstants.java",
    ]),
)
    
java_test(
    name = "cluster-defaults_unittest",
    srcs = glob(["**/ClusterDefaultsTest.java"]),
    deps = [
        "//heron/common/src/java:config-java",
        "//heron/spi/src/java:common-spi-java",
        "//heron/newscheduler/src/java:newscheduler-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "cluster-config_unittest",
    srcs = glob(["**/ClusterConfigTest.java"]),
    deps = [
        ":test-utils-java",
        "//heron/common/src/java:config-java",
        "//heron/spi/src/java:common-spi-java",
        "//heron/newscheduler/src/java:newscheduler-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/testdata/**/*.yaml"]),
    size = "small",
)
