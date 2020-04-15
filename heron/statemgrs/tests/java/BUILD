load("@rules_java//java:defs.bzl", "java_test")

test_deps_files = [
    "//third_party/java:powermock",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
    "@maven//:com_google_guava_guava",
]

spi_deps_files = [
    "//heron/common/src/java:basics-java",
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:network-utils-java",
]

local_statemgr_deps_files = [
    "//heron/statemgrs/src/java:localfs-statemgr-java",
]

zookeeper_statemgr_deps_files = [
    "//heron/statemgrs/src/java:zookeeper-statemgr-java",
    "@maven//:org_apache_curator_curator_framework",
    "@maven//:org_apache_zookeeper_zookeeper",
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
    name = "LocalFileSystemStateManagerTest",
    size = "small",
    srcs = glob(["**/localfs/LocalFileSystemStateManagerTest.java"]),
    deps = localfs_deps_files,
)

java_test(
    name = "ZkUtilsTest",
    size = "small",
    srcs = glob(["**/zookeeper/ZkUtilsTest.java"]),
    deps = zookeeper_deps_files,
)

java_test(
    name = "CuratorStateManagerTest",
    size = "small",
    srcs = glob(["**/zookeeper/curator/CuratorStateManagerTest.java"]),
    deps = zookeeper_deps_files,
)
