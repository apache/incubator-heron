package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

packing_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:packing-spi-java",
]

roundrobin_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/spi/src/java:utils-spi-java",
    ]

java_library(
    name='nulled-packing',
    srcs = glob(
        ["**/NullPacking.java"],
    ),
    deps = packing_deps_files,
)
java_library(
    name='roundrobin-packing',
    srcs = glob(
        ["**/roundrobin/**/*.java"],
    ),
    deps = roundrobin_deps_files,
)
