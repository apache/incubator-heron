load("@rules_java//java:defs.bzl", "java_library", "java_test")

common_deps_files = [
    "//third_party/java:powermock",
    "@maven//:commons_io_commons_io",
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

dlog_deps_files = \
    common_deps_files + \
    spi_deps_files + [
        "@maven//:org_apache_distributedlog_distributedlog_core_shaded",
        "@maven//:io_netty_netty_all",
        "//heron/uploaders/src/java:dlog-uploader-java",
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
    size = "small",
    data = glob(["**/localfs/testdata/*.tar"]),
    test_classes = [
        "org.apache.heron.uploader.localfs.LocalFileSystemConfigTest",
        "org.apache.heron.uploader.localfs.LocalFileSystemContextTest",
        "org.apache.heron.uploader.localfs.LocalFileSystemUploaderTest",
    ],
    runtime_deps = [":localfs-tests"],
)

java_test(
    name = "DlogUploaderTest",
    size = "small",
    srcs = glob(["**/dlog/DlogUploaderTest.java"]),
    deps = dlog_deps_files,
)

java_test(
    name = "S3UploaderTest",
    size = "small",
    srcs = glob(["**/s3/S3UploaderTest.java"]),
    deps = common_deps_files + spi_deps_files + [
        "//heron/uploaders/src/java:s3-uploader-java",
        "//third_party/java:aws-java-sdk",
    ],
)

java_test(
    name = "HdfsUploaderTest",
    size = "small",
    srcs = glob(["**/hdfs/HdfsUploaderTest.java"]),
    deps = hdfs_deps_files,
)

java_test(
    name = "ScpUploaderTest",
    size = "small",
    srcs = glob(["**/scp/ScpUploaderTest.java"]),
    deps = scp_deps_files,
)

java_test(
    name = "GcsUploaderTests",
    size = "small",
    srcs = glob(["**/gcs/GcsUploaderTests.java"]),
    deps = common_deps_files + spi_deps_files + [
        "//heron/uploaders/src/java:gcs-uploader-java",
        "//third_party/java:google-api-services-storage",
    ],
)

java_test(
    name = "HttpUploaderTest",
    size = "small",
    srcs = glob(["**/http/HttpUploaderTest.java"]),
    deps = common_deps_files + spi_deps_files + [
        "//heron/uploaders/src/java:http-uploader-java",
        "@maven//:commons_logging_commons_logging",
        "@maven//:org_apache_httpcomponents_httpcore",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpclient_tests",
        "//heron/common/src/java:basics-java",
    ],
)
