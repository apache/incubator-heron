load("@rules_java//java:defs.bzl", "java_library")

metricscachemgr_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/metricsmgr/src/java:metricsmgr-java",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/spi/src/java:metricsmgr-spi-java",
    "//heron/proto:proto_common_java",
    "//heron/proto:proto_metrics_java",
    "//heron/proto:proto_tmaster_java",
    "//third_party/java:jackson",
    "//third_party/java:cli",
    "//third_party/java:guava",
    "//heron/proto:proto_topology_java",
    "//heron/metricscachemgr/src/java:metricscachemgr-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/proto:proto_packing_plan_java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/instance/src/java:instance-java",
    "//heron/statemgrs/src/java:localfs-statemgr-java",
]

test_deps_files = [
    "//third_party/java:junit4",
    "//heron/spi/src/java:utils-spi-java",
    "@maven//:com_google_guava_guava",
    "//third_party/java:powermock",
    "//third_party/java:mockito",
]

java_library(
    name = "metricscachemgr-tests",
    srcs = glob(["**/*.java"]),
    deps = metricscachemgr_deps_files +
           test_deps_files + [
        "//heron/statemgrs/src/java:null-statemgr-java",
        "//heron/schedulers/src/java:null-scheduler-java",
        "//heron/packing/src/java:roundrobin-packing",
    ],
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.metricscachemgr.metricscache.CacheCoreTest",
        "org.apache.heron.metricscachemgr.metricscache.MetricsCacheQueryUtilsTest",
        "org.apache.heron.metricscachemgr.metricscache.MetricsCacheTest",
    ],
    runtime_deps = [":metricscachemgr-tests"],
)
