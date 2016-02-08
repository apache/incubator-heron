
load("/tools/rules/heron_deps", "heron_java_proto_files")

test_deps_files = [
    "//3rdparty/powermock:powermock-java",
    "@bazel_tools//third_party:mockito",
    "@bazel_tools//third_party:junit4",
]

statemgr_deps_files = \
    heron_java_proto_files() + [
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/statemgrs/src/java:localfs-statemgr-java",
    ]

java_test(
    name = "localfs-statemgr_unittest",
    srcs = glob(
        ["**/localfs/LocalFileSystemStateManagerTest.java"]
    ),
    deps = statemgr_deps_files + test_deps_files,
    size = "small",
)
