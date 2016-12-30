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

java_library(
    name = 'null-uploader-java',
    srcs = glob(["**/NullUploader.java"]),
    deps = uploader_spi_files,
)

java_binary(
    name = 'null-uploader-unshaded',
    srcs = glob(["**/NullUploader.java"]),
    deps = uploader_spi_files,
)

genrule(
    name = 'heron-null-uploader',
    srcs = [":null-uploader-unshaded_deploy.jar"],
    outs = ["heron-null-uploader.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name = 'localfs-uploader-java',
    srcs = glob(["**/localfs/**/*.java"]),
    resources = glob(["**/localfs/**/*.yaml"]),
    deps = uploader_spi_files,
)

java_binary(
    name = 'localfs-uploader-unshaded',
    srcs = glob(["**/localfs/**/*.java"]),
    resources = glob(["**/localfs/**/*.yaml"]),
    deps = uploader_spi_files,
)

genrule(
    name = "heron-localfs-uploader",
    srcs = [":localfs-uploader-unshaded_deploy.jar"],
    outs = ["heron-localfs-uploader.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name = 'hdfs-uploader-java',
    srcs = glob(["**/hdfs/**/*.java"]),
    deps = uploader_spi_files,
)

java_binary(
    name = 'hdfs-uploader-unshaded',
    srcs = glob(["**/hdfs/**/*.java"]),
    deps = uploader_spi_files,
)

genrule(
    name = "heron-hdfs-uploader",
    srcs = [":hdfs-uploader-unshaded_deploy.jar"],
    outs = ["heron-hdfs-uploader.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name = 's3-uploader-java',
    srcs = glob(["**/s3/**/*.java"]),
    deps = s3_deps_files,
)

java_binary(
    name = 's3-uploader-unshaded',
    srcs = glob(["**/s3/**/*.java"]),
    deps = s3_deps_files)

genrule(
    name = "heron-s3-uploader",
    srcs = [":s3-uploader-unshaded_deploy.jar"],
    outs = ["heron-s3-uploader.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name = 'scp-uploader-java',
    srcs = glob(["**/scp/**/*.java"]),
    deps = uploader_spi_files,
)

java_binary(
    name = 'scp-uploader-unshaded',
    srcs = glob(["**/scp/**/*.java"]),
    deps = uploader_spi_files)

genrule(
    name = "heron-scp-uploader",
    srcs = [":scp-uploader-unshaded_deploy.jar"],
    outs = ["heron-scp-uploader.jar"],
    cmd  = "cp $< $@",
)
