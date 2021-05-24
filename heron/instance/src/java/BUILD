load("@rules_java//java:defs.bzl", "java_binary", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

instance_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:utils-java",
        "@maven//:commons_cli_commons_cli",
    ]

java_library(
    name = "instance-java",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/HeronInstance.java"],
    ),
    deps = instance_deps_files,
)

java_binary(
    name = "instance-unshaded",
    srcs = glob(["**/*/*.java"]),
    main_class = "org.apache.heron.instance.HeronInstance",
    deps = instance_deps_files,
)

jar_jar(
    name = "heron-instance",
    input_jar = ":instance-unshaded_deploy.jar",
    rules = "shade.conf",
)
