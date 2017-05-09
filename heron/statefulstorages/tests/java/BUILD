load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

test_deps_files = \
    heron_java_proto_files() + [
      "//heron/spi/src/java:statefulstorage-spi-java",
      "//third_party/java:powermock",
      "//third_party/java:mockito",
      "//third_party/java:junit4"
    ]


localfs_deps_files = [
    "//heron/common/src/java:basics-java",
    "//heron/statefulstorages/src/java:localfs-statefulstorage-java",
]

java_library(
  name = "localfs-tests",
  srcs = glob(["**/localfs/*.java"]),
  deps = test_deps_files + localfs_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.statefulstorage.localfs.LocalFileSystemStorageTest",
  ],
  runtime_deps = [":localfs-tests"],
  size = "small",
)

hdfs_deps_files = [
  "//heron/statefulstorages/src/java:hdfs-statefulstorage-java",
  "@org_apache_hadoop_hadoop_core//jar",
  "@commons_logging_commons_logging//jar",
]

java_library(
  name = "hdfs-tests",
  srcs = glob(["**/hdfs/*.java"]),
  deps = test_deps_files + hdfs_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.statefulstorage.hdfs.HDFSStorageTest",
  ],
  runtime_deps = [ ":hdfs-tests" ],
  size = "small",
)