load("@rules_java//java:defs.bzl", "java_library", "java_test")

packing_deps_files = [
    ":packing-utils",
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/packing/src/java:roundrobin-packing",
    "//heron/packing/src/java:binpacking-packing",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:utils-java",
]

test_deps_files = [
    "//third_party/java:junit4",
    "//third_party/java:mockito",
]

roundrobin_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + \
    test_deps_files + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/spi/src/java:utils-spi-java",
    ]

binpacking_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + \
    test_deps_files + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/spi/src/java:utils-spi-java",
    ]

packing_utils_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + \
    test_deps_files + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/spi/src/java:utils-spi-java",
    ]

java_library(
    name = "packing-utils",
    srcs = glob(
        ["org/apache/heron/packing/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:utils-java",
        "//heron/packing/src/java:binpacking-packing",
        "//heron/packing/src/java:builder",
        "//heron/packing/src/java:roundrobin-packing",
        "//heron/packing/src/java:utils",
        "//heron/proto:proto_topology_java",
        "//heron/spi/src/java:common-spi-java",
        "//heron/spi/src/java:packing-spi-java",
        "//heron/spi/src/java:utils-spi-java",
        "//third_party/java:junit4",
    ],
)

java_test(
    name = "RoundRobinPackingTest",
    size = "small",
    srcs = glob(
        ["**/roundrobin/RoundRobinPackingTest.java"],
    ),
    deps = roundrobin_deps_files,
)

java_test(
    name = "ResourceCompliantRRPackingTest",
    size = "small",
    srcs = glob(
        ["**/roundrobin/ResourceCompliantRRPackingTest.java"],
    ),
    deps = roundrobin_deps_files + [
        "//heron/packing/src/java:utils",
    ],
)

java_test(
    name = "FirstFitDecreasingPackingTest",
    size = "small",
    srcs = glob(
        ["**/binpacking/FirstFitDecreasingPackingTest.java"],
    ),
    deps = binpacking_deps_files + [
        "//heron/packing/src/java:utils",
    ],
)

java_test(
    name = "PackingUtilsTest",
    size = "small",
    srcs = glob(
        ["**/PackingUtilsTest.java"],
    ),
    deps = packing_utils_deps_files + [
        "//heron/packing/src/java:utils",
    ],
)

java_test(
    name = "PackingPlanBuilderTest",
    size = "small",
    srcs = glob(
        ["**/PackingPlanBuilderTest.java"],
    ),
    deps = packing_utils_deps_files + [
        "//heron/packing/src/java:builder",
    ],
)

java_test(
    name = "ScorerTest",
    size = "small",
    srcs = glob(
        ["**/ScorerTest.java"],
    ),
    deps = packing_utils_deps_files + [
        "//heron/packing/src/java:builder",
    ],
)
