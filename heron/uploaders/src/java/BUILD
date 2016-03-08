package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

uploader_spi_files = [
    "//heron/common/src/java:basics-java",
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:uploader-spi-java",
]

packer_deps_files = \
    uploader_spi_files + [
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
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

jarjar_binary(
    name = 'heron-null-uploader',
    src  = ":null-uploader-unshaded_deploy.jar",
    shade = "shade.conf",
)

java_library(
    name = 'packer-uploader-java',
    srcs = glob(["**/packer/**/*.java"]),
    deps = packer_deps_files,
)

java_binary(
    name = 'packer-uploader-unshaded',
    srcs = glob(["**/packer/**/*.java"]),
    deps = packer_deps_files,
)

jarjar_binary(
    name = "heron-packer-uploader",
    src = ":packer-uploader-unshaded_deploy.jar",
    shade = "shade.conf",
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

jarjar_binary(
    name = "heron-localfs-uploader",
    src = ":localfs-uploader-unshaded_deploy.jar",
    shade = "shade.conf",
)
