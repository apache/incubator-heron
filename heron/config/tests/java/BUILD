java_library(
    name = "config_test_library",
    srcs = [
        "com/twitter/heron/config/Constants.java",
    ],
)
    
java_test(
    name = "config_reader_unittest",
    srcs = [
        "com/twitter/heron/config/ConfigReaderTest.java",
    ],
    deps = [
        ":config_test_library",
        "//heron/config/src/java:config-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/config/testdata/**/*.conf"]),
    size = "small",
)

java_test(
    name = "heron_config_reader_unittest",
    srcs = [
        "com/twitter/heron/config/HeronConfigReaderTest.java",
    ],
    deps = [
        ":config_test_library",
        "//heron/config/src/java:config-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = glob(["**/config/testdata/**/*.conf"]),
    size = "small",
)
