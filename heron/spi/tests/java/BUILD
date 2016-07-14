
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/spi/src/java:common-spi-java",
    "//third_party/java:powermock",
    "//third_party/java:junit4",
    "//third_party/java:mockito",
]

util_deps_files = \
    common_deps_files + [
        "//heron/spi/src/java:utils-spi-java", 
    ]

packing_deps_files = \
    common_deps_files + [
        "//heron/spi/src/java:packing-spi-java",
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
    name = "PackingPlanTest",
    srcs = glob(["**/packing/PackingPlanTest.java"]),
    deps = packing_deps_files,
    size = "small",
)

java_test(
    name = "ShellUtilsTest",
    srcs = glob(["**/utils/ShellUtilsTest.java"]),
    deps = util_deps_files,
    size = "small",
)

java_test(
    name = "NetworkUtilsTest",
    srcs = glob(["**/utils/NetworkUtilsTest.java"]),
    deps = util_deps_files,
    size = "small",
)

java_test(
    name = "SchedulerUtilsTest",
    srcs = glob(["**/utils/SchedulerUtilsTest.java"]),
    deps = proto_deps_files, 
    size = "small",
)

java_test(
    name = "TopologyUtilsTest",
    srcs = glob(["**/utils/TopologyUtilsTest.java"]),
    deps = api_deps_files, 
    size = "small",
)

java_test(
    name = "ExceptionInfoTest",
    srcs = glob(["**/ExceptionInfoTest.java"]),
    deps = [
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "MetricsInfoTest",
    srcs = glob(["**/MetricsInfoTest.java"]),
    deps = [
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "MetricsRecordTest",
    srcs = glob(["**/MetricsRecordTest.java"]),
    deps = [
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "ConfigKeysTest",
    srcs = glob(["**/ConfigKeysTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "ConfigDefaultsTest",
    srcs = glob(["**/ConfigDefaultsTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "ConfigKeysDefaultsTest",
    srcs = glob(["**/ConfigKeysDefaultsTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "MiscTest",
    srcs = glob(["**/MiscTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "ClusterDefaultsTest",
    srcs = glob(["**/ClusterDefaultsTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    size = "small",
)

java_test(
    name = "ClusterConfigTest",
    srcs = glob(["**/ClusterConfigTest.java"]),
    deps = [":test-utils-java"] + common_deps_files,
    data = glob(["**/testdata/**/*.yaml"]),
    size = "small",
)

java_test(
    name = "UploaderUtilsTest",
    srcs = glob(["**/utils/UploaderUtilsTest.java"]),
    deps = util_deps_files,
    size = "small",
)
