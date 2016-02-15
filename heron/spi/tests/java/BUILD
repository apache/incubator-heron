
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//heron/spi/src/java:common-spi-java", 
    "@bazel_tools//third_party:junit4",
    "@bazel_tools//third_party:mockito",
]

util_deps_files = \
    common_deps_files + [
        "//heron/spi/src/java:utils-spi-java", 
    ]
    
proto_deps_files = \
    heron_java_proto_files() + \
    util_deps_files

api_deps_files = proto_deps_files + [
    "//heron/api/src/java:api-java", 
]

java_test(
    name = "shell-utils_unittest",
    srcs = glob(["**/common/ShellUtilsTest.java"]),
    deps = common_deps_files, 
    size = "small",
)

java_test(
    name = "http-utils_unittest",
    srcs = glob(["**/common/HttpUtilsTest.java"]),
    deps = common_deps_files,
    size = "small",
)

java_test(
    name = "network-utils_unittest",
    srcs = glob(["**/utils/NetworkUtilsTest.java"]),
    deps = proto_deps_files, 
    size = "small",
)

java_test(
    name = "topology-utils_unittest",
    srcs = glob(["**/utils/TopologyUtilsTest.java"]),
    deps = api_deps_files, 
    size = "small",
)

java_test(
    name = "exception-info_unittest",
    srcs = glob(["**/ExceptionInfoTest.java"]),
    deps = [
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "metrics-info_unittest",
    srcs = glob(["**/MetricsInfoTest.java"]),
    deps = [
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "metrics-record_unittest",
    srcs = glob(["**/MetricsRecordTest.java"]),
    deps = [
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)
