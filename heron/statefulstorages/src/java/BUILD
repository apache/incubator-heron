load("@rules_java//java:defs.bzl", "java_binary", "java_library")

package(default_visibility = ["//visibility:public"])

spi_deps_files = [
    "//heron/spi/src/java:statefulstorage-spi-java",
    "//heron/proto:proto_ckptmgr_java",
    "//heron/proto:proto_physical_plan_java",
]

localfs_deps_files = \
    spi_deps_files + [
        "//heron/common/src/java:basics-java",
        "@com_google_protobuf//:protobuf_java",
    ]

java_library(
    name = "localfs-statefulstorage-java",
    srcs = glob(["**/localfs/*.java"]),
    deps = localfs_deps_files,
)

java_binary(
    name = "localfs-statefulstorage-unshaded",
    srcs = glob(["**/localfs/*.java"]),
    deps = localfs_deps_files,
)

genrule(
    name = "heron-localfs-statefulstorage",
    srcs = [":localfs-statefulstorage-unshaded_deploy.jar"],
    outs = ["heron-localfs-statefulstorage.jar"],
    cmd = "cp $< $@",
)

hdfs_deps_files = \
    spi_deps_files + [
        "//third_party/java:hadoop-core",
        "//heron/common/src/java:basics-java",
    ]

java_library(
    name = "hdfs-statefulstorage-java",
    srcs = glob(["**/hdfs/*.java"]),
    deps = hdfs_deps_files,
)

java_binary(
    name = "hdfs-statefulstorage-unshaded",
    srcs = glob(["**/hdfs/*.java"]),
    deps = hdfs_deps_files,
)

genrule(
    name = "heron-hdfs-statefulstorage",
    srcs = [":hdfs-statefulstorage-unshaded_deploy.jar"],
    outs = ["heron-hdfs-statefulstorage.jar"],
    cmd = "cp $< $@",
)

dlog_deps_files = \
    spi_deps_files + [
        "//third_party/java:dlog-java",
        "//heron/common/src/java:basics-java",
        "//heron/io/dlog/src/java:dlog-lib",
        "@maven//:com_google_guava_guava",
    ]

java_library(
    name = "dlog-statefulstorage-java",
    srcs = glob(["**/dlog/*.java"]),
    deps = dlog_deps_files,
)

java_binary(
    name = "dlog-statefulstorage-unshaded",
    srcs = glob(["**/dlog/*.java"]),
    deps = dlog_deps_files,
)

genrule(
    name = "heron-dlog-statefulstorage",
    srcs = [":dlog-statefulstorage-unshaded_deploy.jar"],
    outs = ["heron-dlog-statefulstorage.jar"],
    cmd = "cp $< $@",
)
