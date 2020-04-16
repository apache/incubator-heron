load("@rules_java//java:defs.bzl", "java_library")

test_deps_files = \
    heron_java_proto_files() + [
        "//heron/ckptmgr/src/java:ckptmgr-java",
        "//heron/spi/src/java:statefulstorage-spi-java",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:test-helpers-java",
        "//third_party/java:powermock",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ]

java_library(
    name = "ckptmgr-tests",
    srcs = glob(["**/ckptmgr/*.java"]),
    deps = test_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.ckptmgr.CheckpointManagerServerTest",
    ],
    runtime_deps = [":ckptmgr-tests"],
)
