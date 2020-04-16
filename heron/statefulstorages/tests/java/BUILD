load("@rules_java//java:defs.bzl", "java_library", "java_test")

test_deps_files = \
    heron_java_proto_files() + [
        "//heron/spi/src/java:statefulstorage-spi-java",
        "//third_party/java:powermock",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ]

localfs_deps_files = [
    "//heron/common/src/java:basics-java",
    "//heron/statefulstorages/src/java:localfs-statefulstorage-java",
]

java_library(
    name = "localfs-tests",
    srcs = glob([
        "**/localfs/*.java",
        "**/StatefulStorageTestContext.java",
    ]),
    deps = test_deps_files + localfs_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.statefulstorage.localfs.LocalFileSystemStorageTest",
    ],
    runtime_deps = [":localfs-tests"],
)

hdfs_deps_files = [
    "//heron/statefulstorages/src/java:hdfs-statefulstorage-java",
    "@maven//:org_apache_hadoop_hadoop_core",
    "@maven//:commons_logging_commons_logging",
]

java_library(
    name = "hdfs-tests",
    srcs = glob([
        "**/hdfs/*.java",
        "**/StatefulStorageTestContext.java",
    ]),
    deps = test_deps_files + hdfs_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.statefulstorage.hdfs.HDFSStorageTest",
    ],
    runtime_deps = [":hdfs-tests"],
)

dlog_deps_files = [
    "//heron/statefulstorages/src/java:dlog-statefulstorage-java",
    "//heron/io/dlog/src/java:dlog-lib",
    "@maven//:com_google_guava_guava",
    "@maven//:org_apache_distributedlog_distributedlog_core_shaded",
    "@maven//:io_netty_netty_all",
]

java_library(
    name = "dlog-tests",
    srcs = glob([
        "**/dlog/*.java",
        "**/StatefulStorageTestContext.java",
    ]),
    deps = test_deps_files + dlog_deps_files,
)

java_test(
    name = "DlogStorageTest",
    size = "small",
    srcs = glob([
        "**/dlog/*.java",
        "**/StatefulStorageTestContext.java",
    ]),
    deps = test_deps_files + dlog_deps_files,
)
