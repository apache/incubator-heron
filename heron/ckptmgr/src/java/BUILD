load("@rules_java//java:defs.bzl", "java_binary", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "ckptmgr-java",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/CheckpointManager.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:utils-java",
        "//heron/proto:proto_ckptmgr_java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java",
        "//heron/proto:proto_topology_java",
        "//heron/spi/src/java:statefulstorage-spi-java",
        "@com_google_protobuf//:protobuf_java",
    ],
)

java_binary(
    name = "ckptmgr-unshaded",
    srcs = glob(["**/CheckpointManager.java"]),
    deps = [
        ":ckptmgr-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:utils-java",
        "//heron/proto:proto_ckptmgr_java",
        "//heron/spi/src/java:statefulstorage-spi-java",
        "@maven//:commons_cli_commons_cli",
    ],
)

jar_jar(
    name = "heron-ckptmgr",
    input_jar = ":ckptmgr-unshaded_deploy.jar",
    rules = "shade.conf",
)
