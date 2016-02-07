licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/heron", "heron_java_proto_files")

java_library(
    name = "instance-lib-java",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/HeronInstance.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//3rdparty/logging:logging-java", 
    ] + heron_java_proto_files(),
)

java_binary(
    name = "instance-unshaded",
    srcs = glob(["**/HeronInstance.java"]),
    main_class = "com.twitter.heron.instance.HeronInstance",
    deps = [
        ":instance-lib-java",
        "//heron/common/src/java:common-java",
        # Heron does not need it, we supply this,
        # since we exclude them explicitly in target "heron_binary".
        # Later we might remove it.
        "//3rdparty/logging:logging-java",
    ] + heron_java_proto_files(),
)

jarjar_binary(
    name = "heron-instance",
    src = ":instance-unshaded_deploy.jar",
    shade = "shade.conf",
)

filegroup(
    name = "aurora-logging-properties",
    srcs = ["aurora_logging.properties"],
)
