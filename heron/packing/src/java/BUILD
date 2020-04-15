load("@rules_java//java:defs.bzl", "java_binary", "java_library")

package(default_visibility = ["//visibility:public"])

packing_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:utils-java",
    "//heron/spi/src/java:packing-spi-java",
    "@maven//:com_google_guava_guava",
    ":builder",
    ":utils",
]

roundrobin_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/spi/src/java:utils-spi-java",
    ]

binpacking_deps_files = \
    heron_java_proto_files() + \
    packing_deps_files + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/spi/src/java:utils-spi-java",
    ]

java_library(
    name = "utils",
    srcs = glob(["**/utils/*.java"]),
    deps = heron_java_proto_files() + [
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:utils-java",
        "//heron/spi/src/java:common-spi-java",
        "//heron/spi/src/java:packing-spi-java",
        "//heron/spi/src/java:utils-spi-java",
        "//heron/api/src/java:api-java-low-level",
    ],
)

java_library(
    name = "builder",
    srcs = glob([
        "**/packing/builder/*.java",
        "**/packing/constraints/*.java",
        "**/packing/exceptions/*.java",
    ]),
    deps = heron_java_proto_files() + [
        "@maven//:com_google_guava_guava",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:packing-spi-java",
        ":utils",
    ],
)

java_library(
    name = "roundrobin-packing",
    srcs = glob([
        "**/roundrobin/**/*.java",
        "**/packing/*.java",
    ]),
    deps = roundrobin_deps_files,
)

java_binary(
    name = "roundrobin-packing-unshaded",
    srcs = glob([
        "**/roundrobin/**/*.java",
        "**/packing/*.java",
    ]),
    deps = roundrobin_deps_files,
)

genrule(
    name = "heron-roundrobin-packing",
    srcs = [":roundrobin-packing-unshaded_deploy.jar"],
    outs = ["heron-roundrobin-packing.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "binpacking-packing",
    srcs = glob([
        "**/binpacking/**/*.java",
        "**/packing/*.java",
    ]),
    deps = binpacking_deps_files,
)

java_binary(
    name = "binpacking-packing-unshaded",
    srcs = glob([
        "**/binpacking/**/*.java",
        "**/packing/*.java",
    ]),
    deps = binpacking_deps_files,
)

genrule(
    name = "heron-binpacking-packing",
    srcs = [":binpacking-packing-unshaded_deploy.jar"],
    outs = ["heron-binpacking-packing.jar"],
    cmd = "cp $< $@",
)
