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
        ":local-filesystem-constants-java",
        "//heron/uploaders/src/java:localfs-uploader-java",
    ]

hdfs_deps_files = \
    common_deps_files + \
    spi_deps_files + [
        "//heron/uploaders/src/java:hdfs-uploader-java",
    ]

java_library(
    name = "local-filesystem-constants-java",
    srcs = glob(["**/localfs/LocalFileSystemConstantsTest.java"]),
)

java_test(
    name = "LocalFileSystemConfigTest",
    srcs = glob(["**/localfs/LocalFileSystemConfigTest.java"]),
    deps = localfs_deps_files,
    size = "small",
)

java_test(
    name = "LocalFileSystemUploaderTest",
    srcs = glob(["**/localfs/LocalFileSystemUploaderTest.java"]),
    deps = localfs_deps_files,
    size = "small",
    data = glob(["**/localfs/testdata/*.tar"]),
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
