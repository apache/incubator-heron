load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//3rdparty/powermock:powermock-java",
    "//3rdparty/commons:commons-io-java",
    "@bazel_tools//third_party:mockito",
    "@bazel_tools//third_party:junit4",
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

java_library(
    name = "local-filesystem-constants-java",
    srcs = glob(["**/localfs/LocalFileSystemConstantsTest.java"]),
)

java_test(
    name = "local-filesystem-config_unittest",
    srcs = glob(["**/localfs/LocalFileSystemConfigTest.java"]),
    deps = localfs_deps_files,
    size = "small",
)

java_test(
    name = "local-filesystem-uploader_unittest",
    srcs = glob(["**/localfs/LocalFileSystemUploaderTest.java"]),
    deps = localfs_deps_files,
    size = "small",
    data = glob(["**/localfs/testdata/*.tar"]),
)

java_test(
    name = "s3-uploader_unittest",
    srcs = glob(["**/s3/S3UploaderTest.java"]),
    size = "small",
    deps = common_deps_files + spi_deps_files + [
        "//heron/uploaders/src/java:s3-uploader-java",
        "//3rdparty/aws:aws-java-sdk"
    ],
)
