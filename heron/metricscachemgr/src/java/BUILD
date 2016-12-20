licenses(["notice"])

package(default_visibility = ["//visibility:public"])

metricscachemgr_deps_files = [
           "//heron/spi/src/java:common-spi-java",
          # "//heron/spi/src/java:slamgr-spi-java",
           "//heron/api/src/java:api-java",
           "//heron/common/src/java:common-java",
           "//heron/spi/src/java:metricsmgr-spi-java",
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
          # "@javax_ws_rs_javax_ws_rs_api//jar",
          # "@org_glassfish_jersey_core_jersey_client//jar",
          # "@org_glassfish_jersey_core_jersey_common//jar",
          # "@org_glassfish_jersey_media_jersey_media_json_jackson//jar",
           "//heron/proto:proto_topology_java",
          # "@org_glassfish_hk2_hk2_api//jar",
          # "@org_glassfish_jersey_bundles_repackaged_jersey_guava//jar",
          # "@org_glassfish_hk2_hk2_utils//jar",
          # "@org_glassfish_hk2_hk2_locator//jar",
          # "@javax_annotation_javax_annotation_api//jar",
          # "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider//jar",
          # "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_base//jar",
          # "@org_glassfish_jersey_ext_jersey_entity_filtering//jar",
          # "@com_fasterxml_jackson_module_jackson_module_jaxb_annotations//jar",
           "//heron/spi/src/java:utils-spi-java",
           "@commons_cli_commons_cli//jar",
           "//heron/spi/src/java:statemgr-spi-java",
          # "@org_glassfish_hk2_external_javax_inject//jar",
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

genrule(
    name = "heron-metricscachemgr",
    srcs = [":metricscachemgr-unshaded_deploy.jar"],
    outs = ["heron-metricscachemgr.jar"],
    cmd  = "cp $< $@",
)
