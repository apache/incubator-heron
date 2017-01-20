licenses(["notice"])

package(default_visibility = ["//visibility:public"])

metricscachemgr_deps_files = [
           "//heron/spi/src/java:common-spi-java",
           "//heron/api/src/java:api-java",
           "//heron/common/src/java:common-java",
           "//heron/spi/src/java:metricsmgr-spi-java",
           "//heron/statemgrs/src/java:localfs-statemgr-java",
           "//heron/proto:proto_common_java",
           "//heron/proto:proto_metrics_java",
           "//heron/proto:proto_tmaster_java",
           "//heron/metricsmgr/src/thrift:thrift_scribe_java",
           "//heron/metricsmgr/src/java:metricsmgr-java",
           "//third_party/java:jackson",
           "@org_yaml_snakeyaml//jar",
           "@com_google_protobuf_protobuf_java//jar",
           "@org_apache_thrift_libthrift//jar",
           "//third_party/java:logging", # slf4j is only used for thrift in scribe-sink
           "//heron/proto:proto_topology_java",
           "//heron/spi/src/java:utils-spi-java",
           "@commons_cli_commons_cli//jar",
           "//heron/spi/src/java:statemgr-spi-java",
           "//heron/proto:proto_packing_plan_java",
           "//heron/spi/src/java:packing-spi-java",
]

java_library(
    name = "metricscachemgr-java",
    srcs = glob(
        ["**/*.java"],
         ),
    deps = metricscachemgr_deps_files,
)

java_binary(
    name = "metricscachemgr-unshaded",
    srcs = glob(["**/*.java"]),
    deps = metricscachemgr_deps_files,
)

java_binary(
    name = "metricscache-queryclient-unshaded",
    srcs = glob(["**/*.java"]),
    deps = metricscachemgr_deps_files,
    main_class="com.twitter.heron.metricscachemgr.ExampleMetricsQuery"
)

genrule(
    name = "heron-metricscachemgr",
    srcs = [":metricscachemgr-unshaded_deploy.jar"],
    outs = ["heron-metricscachemgr.jar"],
    cmd  = "cp $< $@",
)
