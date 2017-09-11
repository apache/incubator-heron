package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

spi_deps_files = [
    "//heron/spi/src/java:statefulstorage-spi-java",
    "//heron/proto:proto_ckptmgr_java",
    "//heron/proto:proto_physical_plan_java",
]

hdfs_deps_files = \
    spi_deps_files + [
        "//third_party/java:hadoop-core",
        "//heron/common/src/java:basics-java",
    ]

localfs_deps_files = \
    spi_deps_files + [
        "//heron/common/src/java:basics-java",
        "@com_google_protobuf_protobuf_java//jar",
    ]

java_library(
    name='localfs-statefulstorage-java',
    srcs = glob(["**/localfs/*.java"]),
    deps = localfs_deps_files,
)

java_binary(
    name='localfs-statefulstorage-unshaded',
    srcs = glob(["**/localfs/*.java"]),
    deps = localfs_deps_files,
)

genrule(
    name = "heron-localfs-statefulstorage",
    srcs = [":localfs-statefulstorage-unshaded_deploy.jar"],
    outs = ["heron-localfs-statefulstorage.jar"],
    cmd  = "cp $< $@",
)

java_library(
    name='hdfs-statefulstorage-java',
    srcs = glob(["**/hdfs/*.java"]),
    deps = hdfs_deps_files,
)

java_binary(
    name='hdfs-statefulstorage-unshaded',
    srcs = glob(["**/hdfs/*.java"]),
    deps = hdfs_deps_files,
)

genrule(
    name = "heron-hdfs-statefulstorage",
    srcs = [":hdfs-statefulstorage-unshaded_deploy.jar"],
    outs = ["heron-hdfs-statefulstorage.jar"],
    cmd  = "cp $< $@",
)
