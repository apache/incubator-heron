load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//third_party/java:powermock",
    "@commons_io_commons_io//jar",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
]

spi_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:uploader-spi-java",
    "//heron/spi/src/java:utils-spi-java",
]

localfs_deps_files = \
    common_deps_files + \
    spi_deps_files + [
        "//heron/uploaders/src/java:localfs-uploader-java",
        "//heron/common/src/java:basics-java",
    ]

hdfs_deps_files = \
    common_deps_files + \
    spi_deps_files + [
        "//heron/uploaders/src/java:hdfs-uploader-java",
    ]

scp_deps_files = \
    common_deps_files + \
    spi_deps_files + [
        "//heron/uploaders/src/java:scp-uploader-java",
    ]


java_library(
    name = "localfs-tests",
    srcs = glob(["**/localfs/*.java"]),
    deps = localfs_deps_files,
)

java_tests(
  test_classes = [
    "com.twitter.heron.uploader.localfs.LocalFileSystemConfigTest",
    "com.twitter.heron.uploader.localfs.LocalFileSystemUploaderTest",
  ],
  runtime_deps = [ ":localfs-tests" ],
  data = glob(["**/localfs/testdata/*.tar"]),
  size = "small",
)

java_test(
    name = "S3UploaderTest",
    srcs = glob(["**/s3/S3UploaderTest.java"]),
    size = "small",
    deps = common_deps_files + spi_deps_files + [
        "//heron/uploaders/src/java:s3-uploader-java",
        "//third_party/java:aws-java-sdk"
    ],
)

java_test(
    name = "HdfsUploaderTest",
    srcs = glob(["**/hdfs/HdfsUploaderTest.java"]),
    deps = hdfs_deps_files,
    size = "small",
)

java_test(
    name = "ScpUploaderTest",
    srcs = glob(["**/scp/ScpUploaderTest.java"]),
    deps = scp_deps_files,
    size = "small",
)
