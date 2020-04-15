load("@rules_java//java:defs.bzl", "java_binary", "java_library")

package(default_visibility = ["//visibility:public"])

uploader_spi_files = [
    "//heron/common/src/java:basics-java",
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:uploader-spi-java",
    "//heron/spi/src/java:shell-utils-java",
    "//heron/spi/src/java:uploader-utils-java",
]

s3_deps_files = \
    uploader_spi_files + [
        "//third_party/java:aws-java-sdk",
        "//third_party/java:guava",
    ]

gcs_deps_files = \
    uploader_spi_files + [
        "//third_party/java:google-api-services-storage",
        "//third_party/java:guava",
    ]

dlog_deps_files = \
    uploader_spi_files + [
        "@maven//:org_apache_distributedlog_distributedlog_core_shaded",
        "@maven//:io_netty_netty_all",
        "//heron/io/dlog/src/java:dlog-lib",
    ]

http_uploader_deps_files = \
    uploader_spi_files + [
        "@maven//:org_apache_httpcomponents_httpmime",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
        "@maven//:org_apache_commons_commons_lang3",
        "//third_party/java:guava",
    ]

java_library(
    name = "null-uploader-java",
    srcs = glob(["**/NullUploader.java"]),
    deps = uploader_spi_files,
)

java_binary(
    name = "null-uploader-unshaded",
    srcs = glob(["**/NullUploader.java"]),
    deps = uploader_spi_files,
)

genrule(
    name = "heron-null-uploader",
    srcs = [":null-uploader-unshaded_deploy.jar"],
    outs = ["heron-null-uploader.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "localfs-uploader-java",
    srcs = glob(["**/localfs/**/*.java"]),
    deps = uploader_spi_files,
)

java_binary(
    name = "localfs-uploader-unshaded",
    srcs = glob(["**/localfs/**/*.java"]),
    deps = uploader_spi_files,
)

genrule(
    name = "heron-localfs-uploader",
    srcs = [":localfs-uploader-unshaded_deploy.jar"],
    outs = ["heron-localfs-uploader.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "hdfs-uploader-java",
    srcs = glob(["**/hdfs/**/*.java"]),
    deps = uploader_spi_files,
)

java_binary(
    name = "hdfs-uploader-unshaded",
    srcs = glob(["**/hdfs/**/*.java"]),
    deps = uploader_spi_files,
)

genrule(
    name = "heron-hdfs-uploader",
    srcs = [":hdfs-uploader-unshaded_deploy.jar"],
    outs = ["heron-hdfs-uploader.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "dlog-uploader-java",
    srcs = glob(["**/dlog/**/*.java"]),
    deps = dlog_deps_files,
)

java_binary(
    name = "dlog-uploader-unshaded",
    srcs = glob(["**/dlog/**/*.java"]),
    deps = dlog_deps_files,
)

genrule(
    name = "heron-dlog-uploader",
    srcs = [":dlog-uploader-unshaded_deploy.jar"],
    outs = ["heron-dlog-uploader.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "s3-uploader-java",
    srcs = glob(["**/s3/**/*.java"]),
    deps = s3_deps_files,
)

java_binary(
    name = "s3-uploader-unshaded",
    srcs = glob(["**/s3/**/*.java"]),
    deps = s3_deps_files,
)

genrule(
    name = "heron-s3-uploader",
    srcs = [":s3-uploader-unshaded_deploy.jar"],
    outs = ["heron-s3-uploader.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "scp-uploader-java",
    srcs = glob(["**/scp/**/*.java"]),
    deps = uploader_spi_files,
)

java_binary(
    name = "scp-uploader-unshaded",
    srcs = glob(["**/scp/**/*.java"]),
    deps = uploader_spi_files,
)

genrule(
    name = "heron-scp-uploader",
    srcs = [":scp-uploader-unshaded_deploy.jar"],
    outs = ["heron-scp-uploader.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "gcs-uploader-java",
    srcs = glob(["**/gcs/**/*.java"]),
    deps = gcs_deps_files,
)

java_binary(
    name = "gcs-uploader-unshaded",
    srcs = glob(["**/gcs/**/*.java"]),
    deps = gcs_deps_files,
)

genrule(
    name = "heron-gcs-uploader",
    srcs = [":gcs-uploader-unshaded_deploy.jar"],
    outs = ["heron-gcs-uploader.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "http-uploader-java",
    srcs = glob(["**/http/**/*.java"]),
    deps = http_uploader_deps_files,
)

java_binary(
    name = "http-uploader-unshaded",
    srcs = glob(["**/http/**/*.java"]),
    deps = http_uploader_deps_files,
)

genrule(
    name = "heron-http-uploader",
    srcs = [":http-uploader-unshaded_deploy.jar"],
    outs = ["heron-http-uploader.jar"],
    cmd = "cp $< $@",
)
