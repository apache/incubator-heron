load("@rules_java//java:defs.bzl", "java_library", "java_test")

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
    ],
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.spi.utils.ShellUtilsTest",
        "org.apache.heron.spi.utils.NetworkUtilsTest",
        "org.apache.heron.spi.utils.UploaderUtilsTest",
    ],
    runtime_deps = [":utils-tests"],
)

java_test(
    name = "PackingPlanTest",
    size = "small",
    srcs = glob(["**/packing/PackingPlanTest.java"]),
    deps = packing_deps_files,
)

java_test(
    name = "ResourceTest",
    size = "small",
    srcs = glob(["**/packing/ResourceTest.java"]),
    deps = packing_deps_files,
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
    size = "small",
    test_classes = [
        "org.apache.heron.spi.metricsmgr.metrics.ExceptionInfoTest",
        "org.apache.heron.spi.metricsmgr.metrics.MetricsInfoTest",
        "org.apache.heron.spi.metricsmgr.metrics.MetricsRecordTest",
    ],
    runtime_deps = [":metrics-tests"],
)

java_library(
    name = "common-tests",
    srcs = glob(["**/common/*.java"]),
    deps = common_deps_files,
)

java_tests(
    size = "small",
    data = glob(["**/testdata/**/*.yaml"]),
    test_classes = [
        "org.apache.heron.spi.common.ConfigLoaderTest",
        "org.apache.heron.spi.common.ConfigTest",
        "org.apache.heron.spi.common.ContextTest",
        "org.apache.heron.spi.common.KeysTest",
        "org.apache.heron.spi.common.TokenSubTest",
    ],
    runtime_deps = [":common-tests"],
)
