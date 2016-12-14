load("/tools/rules/java_tests", "java_tests")
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
        "//heron/spi/src/java:utils-spi-java",
    ]

proto_deps_files = \
    heron_java_proto_files() + \
    util_deps_files

api_deps_files = proto_deps_files + [
    "//heron/api/src/java:api-java", 
]

java_library(
    name = "utils-tests",
    srcs = glob(["**/utils/*.java"]),
    deps = api_deps_files + [ 
        "//heron/spi/src/java:packing-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
    ]
)

java_tests(
    test_classes = [
        "com.twitter.heron.spi.utils.ShellUtilsTest",
        "com.twitter.heron.spi.utils.NetworkUtilsTest",
        "com.twitter.heron.spi.utils.TopologyUtilsTest",
        "com.twitter.heron.spi.utils.UploaderUtilsTest",
    ],
    runtime_deps = [ ":utils-tests" ],
    size = "small",
)

java_test(
    name = "PackingPlanTest",
    srcs = glob(["**/packing/PackingPlanTest.java"]),
    deps = packing_deps_files,
    size = "small",
)

java_test(
    name = "ResourceTest",
    srcs = glob(["**/packing/ResourceTest.java"]),
    deps = packing_deps_files,
    size = "small",
)


java_library(
    name = "metrics-tests",
    srcs = glob(["**/metrics/*.java"]),
    deps = [
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//third_party/java:junit4",
    ],
)

java_tests(
    test_classes = [
        "com.twitter.heron.spi.metricsmgr.metrics.ExceptionInfoTest",
        "com.twitter.heron.spi.metricsmgr.metrics.MetricsInfoTest",
        "com.twitter.heron.spi.metricsmgr.metrics.MetricsRecordTest",
    ],
    runtime_deps = [ ":metrics-tests" ],
    size = "small",
)

java_library(
    name = "common-tests",
    srcs = glob(["**/common/*.java"]),
    deps = common_deps_files,
)

java_tests(
    test_classes = [
        "com.twitter.heron.spi.common.ConfigKeysTest",
        "com.twitter.heron.spi.common.ConfigDefaultsTest",
        "com.twitter.heron.spi.common.ConfigKeysDefaultsTest",
        "com.twitter.heron.spi.common.MiscTest",
        "com.twitter.heron.spi.common.ClusterDefaultsTest",
        "com.twitter.heron.spi.common.ClusterConfigTest",
    ],
    runtime_deps = [ ":common-tests" ],
    data = glob(["**/testdata/**/*.yaml"]),
    size = "small",
)
