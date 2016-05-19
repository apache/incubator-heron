
load("/tools/rules/heron_deps", "heron_java_proto_files")

test_deps_files = [
    "//3rdparty/java:powermock",
    "@bazel_tools//third_party:mockito",
    "@bazel_tools//third_party:junit4",
    "@guava//jar",
]

spi_deps_files = [
    "//heron/common/src/java:basics-java", 
    "//heron/spi/src/java:common-spi-java", 
    "//heron/spi/src/java:statemgr-spi-java",
]

local_statemgr_deps_files = [
    "//heron/statemgrs/src/java:localfs-statemgr-java",
]

zookeeper_statemgr_deps_files = [
    "//heron/statemgrs/src/java:zookeeper-statemgr-java",
    "@curator-framework//jar",
    "@zookeeper//jar",
]

localfs_deps_files = \
    heron_java_proto_files() + \
    local_statemgr_deps_files + \
    spi_deps_files + \
    test_deps_files

zookeeper_deps_files = \
    heron_java_proto_files() + \
    zookeeper_statemgr_deps_files + \
    spi_deps_files + \
    test_deps_files

java_test(
    name = "localfs-statemgr_unittest",
    srcs = glob(["**/localfs/LocalFileSystemStateManagerTest.java"]),
    deps = localfs_deps_files,
    size = "small",
)

java_test(
    name = "zk_utils_unittest",
    srcs = glob(["**/zookeeper/ZkUtilsTest.java"]),
    deps = zookeeper_deps_files,
    size = "small",
)

java_test(
    name = "zk-statemgr_unittest",
    srcs = glob(["**/zookeeper/curator/CuratorStateManagerTest.java"]),
    deps = zookeeper_deps_files,
    size = "small",
)
