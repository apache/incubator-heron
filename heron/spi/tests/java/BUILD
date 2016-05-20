
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/spi/src/java:common-spi-java",
    "//3rdparty/java:powermock",
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

java_library(
    name = "test-utils-java",
    srcs = glob([
        "**/TestConstants.java",
    ]),
)

java_test(
    name = "shell-utils_unittest",
    srcs = glob(["**/common/ShellUtilsTest.java"]),
    deps = common_deps_files, 
    size = "small",
)

java_test(
    name = "packing-plan_unittest",
    srcs = glob(["**/common/PackingPlanTest.java"]),
    deps = common_deps_files,
    size = "small",
)

java_test(
    name = "network-utils_unittest",
    srcs = glob(["**/common/NetworkUtilsTest.java"]),
    deps = common_deps_files,
    size = "small",
)

java_test(
    name = "scheduler-utils_unittest",
    srcs = glob(["**/utils/SchedulerUtilsTest.java"]),
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

java_test(
    name = "config-keys_unittest",
    srcs = glob(["**/ConfigKeysTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "config-defaults_unittest",
    srcs = glob(["**/ConfigDefaultsTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "config-keys-defaults_unittest",
    srcs = glob(["**/ConfigKeysDefaultsTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "misc_unittest",
    srcs = glob(["**/MiscTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "cluster-defaults_unittest",
    srcs = glob(["**/ClusterDefaultsTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "cluster-config_unittest",
    srcs = glob(["**/ClusterConfigTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    data = glob(["**/testdata/**/*.yaml"]),
    size = "small",
)

java_test(
    name = "uploader-utils_unittest",
    srcs = glob(["**/utils/UploaderUtilsTest.java"]),
    deps = util_deps_files,
    size = "small",
)
