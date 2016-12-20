
load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")


metricscachemgr_deps_files = [
    "//heron/spi/src/java:common-spi-java",
#    "//heron/spi/src/java:slamgr-spi-java",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:common-java",
    "//heron/spi/src/java:metricsmgr-spi-java",
    "//heron/proto:proto_common_java",
    "//heron/proto:proto_metrics_java",
    "//heron/proto:proto_tmaster_java",
    "//heron/metricsmgr/src/thrift:thrift_scribe_java",
    "//third_party/java:jackson",
    "@org_yaml_snakeyaml//jar",
    "@com_google_protobuf_protobuf_java//jar",
    "@org_apache_thrift_libthrift//jar",
    "//third_party/java:logging", # slf4j is only used for thrift in scribe-sink
#    "@javax_ws_rs_javax_ws_rs_api//jar",
#    "@org_glassfish_jersey_core_jersey_client//jar",
#    "@org_glassfish_jersey_core_jersey_common//jar",
#    "@org_glassfish_jersey_media_jersey_media_json_jackson//jar",
    "//heron/proto:proto_topology_java",
    "//heron/metricscachemgr/src/java:metricscachemgr-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/proto:proto_packing_plan_java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/instance/src/java:instance-java",
]

test_deps_files = [
    "//third_party/java:junit4",
    "//heron/spi/src/java:utils-spi-java",
    "@com_google_guava_guava//jar",
    "//third_party/java:powermock",
    "//third_party/java:mockito",
  ]


java_library(
    name = "metricscachemgr-tests",
    srcs = glob(["**/*.java"]),
    deps = metricscachemgr_deps_files +\
           test_deps_files
           + [
               "//heron/statemgrs/src/java:null-statemgr-java",
               "//heron/schedulers/src/java:null-scheduler-java",
               "//heron/packing/src/java:roundrobin-packing",
             ]
)



java_tests(
   test_classes = [
       "com.twitter.heron.metricscachemgr.ColletorMetricsTest",
       "com.twitter.heron.metricscachemgr.metricscache.TimeBucketTest",
       "com.twitter.heron.metricscachemgr.metricscache.MetricTest",
       "com.twitter.heron.metricscachemgr.metricscache.InstanceMetricsTest",
       "com.twitter.heron.metricscachemgr.metricscache.ComponentMetricsTest",
       "com.twitter.heron.metricscachemgr.metricscache.MetricsCacheTest",
     ],
     runtime_deps = [ ":metricscachemgr-tests" ],
     size = "small",
)
