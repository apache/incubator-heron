
load("/tools/rules/heron", "heron_java_proto_files")

java_test(
    name = "localfs-statemgr_unittest",
    srcs = glob(
        ["**/localfs/LocalFileSystemStateManagerTest.java"]
    ),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//heron/statemgrs/src/java:localfs-statemgr-java",
        "//3rdparty/powermock:powermock-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ] + heron_java_proto_files(),
    size = "small",
)
