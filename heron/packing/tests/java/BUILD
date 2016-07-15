
load("/tools/rules/heron_deps", "heron_java_proto_files")

packing_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/packing/src/java:roundrobin-packing",
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

java_test(
    name = "RoundRobinPackingTest",
    srcs = glob(
        ["**/roundrobin/RoundRobinPackingTest.java"]
    ),
    deps = roundrobin_deps_files,
    size = "small",
)
