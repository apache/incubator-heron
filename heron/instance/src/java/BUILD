licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

java_library(
    name = "instance-lib-java",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/HeronInstance.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java", 
        "//heron/proto:proto_tuple_java", 
        "//3rdparty/logging:logging-java", 
        "//3rdparty/protobuf:protobuf-java", 
    ],
)

java_binary(
    name = "instance-unshaded",
    srcs = glob(["**/HeronInstance.java"]),
    main_class = "com.twitter.heron.instance.HeronInstance",
    deps = [
        ":instance-lib-java",
        "//heron/common/src/java:common-java",
        "//heron/proto:proto-java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_tuple_java",
        # Heron does not need it, we supply this,
        # since we exclude them explicitly in target "heron_binary".
        # Later we might remove it.
        "//3rdparty/logging:logging-java",
    ],
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
