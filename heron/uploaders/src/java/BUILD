package(default_visibility = ["//visibility:public"])

uploader_spi_files = [
    "//heron/spi/src/java:uploader-spi-java",
]

packer_deps_files = \
    uploader_spi_files + [
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
    ]

java_library(
    name='nulled-uploader',
    srcs = glob(
        ["**/nulled/**/*.java"],
    ),
    deps = uploader_spi_files,
)

java_library(
    name='packer-uploader',
    srcs = glob(
        ["**/packer/**/*.java"],
    ),
    deps = packer_deps_files,
)

java_library(
    name='localfs-uploader',
    srcs = glob(
        ["**/localfs/**/*.java"],
    ),
    deps = uploader_spi_files,
)
