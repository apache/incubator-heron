
load("/tools/rules/heron_deps", "heron_java_proto_files")

packing_deps_files = [
    ":packing-utils",
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/packing/src/java:roundrobin-packing",
    "//heron/packing/src/java:binpacking-packing",
    "//heron/common/src/java:basics-java"
]

test_deps_files = [
    "//third_party/java:junit4",
    "//third_party/java:mockito",
]

roundrobin_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + \
    test_deps_files + [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:utils-spi-java",
    ]

binpacking_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + \
    test_deps_files + [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:utils-spi-java",
    ]

packing_utils_deps_files = \
     heron_java_proto_files() + \
        packing_deps_files + \
        test_deps_files + [
            "//heron/api/src/java:api-java",
            "//heron/spi/src/java:utils-spi-java",
        ]

java_library(
    name = "packing-utils",
    srcs = glob(
        ["com/twitter/heron/packing/*.java"]
    ),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/packing/src/java:binpacking-packing",
        "//heron/packing/src/java:builder",
        "//heron/packing/src/java:roundrobin-packing",
        "//heron/packing/src/java:utils",
        "//heron/spi/src/java:packing-spi-java",
        "//third_party/java:junit4",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_topology_java",
        "//heron/spi/src/java:common-spi-java",
        "//heron/spi/src/java:utils-spi-java",
    ],
)

java_test(
    name = "RoundRobinPackingTest",
    srcs = glob(
        ["**/roundrobin/RoundRobinPackingTest.java"]
    ),
    deps = roundrobin_deps_files,
    size = "small",
)

java_test(
    name = "ResourceCompliantRRPackingTest",
    srcs = glob(
       ["**/roundrobin/ResourceCompliantRRPackingTest.java"]
    ),
    deps = roundrobin_deps_files +
           ["//heron/packing/src/java:utils"],
    size = "small",
)

java_test(
    name = "FirstFitDecreasingPackingTest",
    srcs = glob(
        ["**/binpacking/FirstFitDecreasingPackingTest.java"]
    ),
    deps = binpacking_deps_files +
           ["//heron/packing/src/java:utils"],
    size = "small",
)

java_test(
    name = "PackingUtilsTest",
    srcs = glob(
        ["**/PackingUtilsTest.java"]
    ),
    deps = packing_utils_deps_files +
           ["//heron/packing/src/java:utils"],
    size = "small",
)

java_test(
    name = "PackingPlanBuilderTest",
    srcs = glob(
        ["**/PackingPlanBuilderTest.java"]
    ),
    deps = packing_utils_deps_files +
           ["//heron/packing/src/java:builder"],
    size = "small",
)

java_test(
    name = "ScorerTest",
    srcs = glob(
        ["**/ScorerTest.java"]
    ),
    deps = packing_utils_deps_files +
           ["//heron/packing/src/java:builder"],
    size = "small",
)
