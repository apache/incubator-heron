test_deps_files = \
    heron_java_proto_files() + [
      "//heron/api/src/java:api-java-low-level",
      "//heron/api/src/java:classification",
      "//heron/common/src/java:basics-java",
      "//heron/common/src/java:config-java",
      "//heron/common/src/java:network-java",
      "//heron/common/src/java:utils-java",
      "//heron/spi/src/java:common-spi-java",
      "//heron/spi/src/java:packing-spi-java",
      "//heron/spi/src/java:statemgr-spi-java",
      "//heron/spi/src/java:utils-spi-java",
      "//heron/spi/src/java:metricsmgr-spi-java",
      "//heron/scheduler-core/src/java:scheduler-java",
      "//third_party/java:jackson",
      "@commons_cli_commons_cli//jar",
      "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider//jar",
      "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_base//jar",
      "@com_fasterxml_jackson_module_jackson_module_jaxb_annotations//jar",
      "@com_jayway_jsonpath//jar",
      "@net_minidev_json_smart//jar",
      "@javax_ws_rs_javax_ws_rs_api//jar",
      "@javax_annotation_javax_annotation_api//jar",
      "@javax_inject_javax_inject//jar",
      "@org_glassfish_jersey_core_jersey_client//jar",
      "@org_glassfish_jersey_core_jersey_common//jar",
      "@org_glassfish_jersey_ext_jersey_entity_filtering//jar",
      "@org_glassfish_jersey_media_jersey_media_json_jackson//jar",
      "@org_glassfish_jersey_bundles_repackaged_jersey_guava//jar",
      "@org_glassfish_hk2_hk2_api//jar",
      "@org_glassfish_hk2_hk2_utils//jar",
      "@org_glassfish_hk2_hk2_locator//jar",
      "@org_yaml_snakeyaml//jar",
      "@com_google_guava_guava//jar",
      "@com_google_inject_guice//jar",
      "@com_google_inject_extensions_guice_assistedinject//jar",
      "@com_microsoft_dhalion//jar",
      "@aopalliance_aopalliance//jar",
      "@org_apache_commons_commons_math3//jar",
      "@tech_tablesaw//jar",
      "@it_unimi_dsi_fastutil//jar",
      "@org_roaringbitmap//jar",
    ]

java_library(
    name = "healthmgr-tests",
    srcs = glob(["**/*.java"]),
    deps = test_deps_files,
)

java_tests(
    test_classes = [
        "org.apache.heron.healthmgr.HealthManagerTest",
        "org.apache.heron.healthmgr.HealthPolicyConfigReaderTest",

        "org.apache.heron.healthmgr.common.PackingPlanProviderTest",
        "org.apache.heron.healthmgr.common.TopologyProviderTest",

        "org.apache.heron.healthmgr.detectors.BackPressureDetectorTest",
        "org.apache.heron.healthmgr.detectors.GrowingWaitQueueDetectorTest",
        "org.apache.heron.healthmgr.detectors.LargeWaitQueueDetectorTest",
        "org.apache.heron.healthmgr.detectors.ProcessingRateSkewDetectorTest",
        "org.apache.heron.healthmgr.detectors.WaitQueueSkewDetectorTest",

        "org.apache.heron.healthmgr.diagnosers.DataSkewDiagnoserTest",
        "org.apache.heron.healthmgr.diagnosers.SlowInstanceDiagnoserTest",
        "org.apache.heron.healthmgr.diagnosers.UnderProvisioningDiagnoserTest",

        "org.apache.heron.healthmgr.resolvers.ScaleUpResolverTest",

        "org.apache.heron.healthmgr.sensors.BackPressureSensorTest",
        "org.apache.heron.healthmgr.sensors.BufferSizeSensorTest",
        "org.apache.heron.healthmgr.sensors.ExecuteCountSensorTest",
        "org.apache.heron.healthmgr.sensors.MetricsCacheMetricsProviderTest",
        "org.apache.heron.healthmgr.sensors.TrackerMetricsProviderTest",
    ],
    runtime_deps = [
        ":healthmgr-tests",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)
