load("@rules_java//java:defs.bzl", "java_binary", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

files = glob(
    ["**/*.java"],
    exclude = ["**/HealthManager.java"],
)

healthmgr_deps_files = [
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
    "@maven//:commons_cli_commons_cli",
    "@maven//:com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider",
    "@maven//:com_fasterxml_jackson_jaxrs_jackson_jaxrs_base",
    "@maven//:com_fasterxml_jackson_module_jackson_module_jaxb_annotations",
    "@maven//:com_jayway_jsonpath_json_path",
    "@maven//:net_minidev_json_smart",
    "@maven//:javax_ws_rs_javax_ws_rs_api",
    "@maven//:javax_annotation_javax_annotation_api",
    "@maven//:org_glassfish_hk2_external_javax_inject",
    "@maven//:org_glassfish_jersey_core_jersey_client",
    "@maven//:org_glassfish_jersey_core_jersey_common",
    "@maven//:org_glassfish_jersey_ext_jersey_entity_filtering",
    "@maven//:org_glassfish_jersey_media_jersey_media_json_jackson",
    "@maven//:org_glassfish_jersey_bundles_repackaged_jersey_guava",
    "@maven//:org_glassfish_hk2_hk2_api",
    "@maven//:org_glassfish_hk2_hk2_utils",
    "@maven//:org_glassfish_hk2_hk2_locator",
    "@maven//:org_yaml_snakeyaml",
    "@maven//:com_google_guava_guava",
    "@maven//:com_google_inject_guice",
    "@maven//:com_google_inject_extensions_guice_assistedinject",
    "@maven//:com_microsoft_dhalion_dhalion",
    "@maven//:aopalliance_aopalliance",
    "@maven//:org_apache_commons_commons_math3",
    "@maven//:tech_tablesaw_tablesaw_core",
    "@maven//:it_unimi_dsi_fastutil",
    "@maven//:org_roaringbitmap_RoaringBitmap",
]

filegroup(
    name = "healthmgr-main",
    srcs = glob(["**/HealthManager.java"]),
)

java_library(
    name = "healthmgr-java",
    srcs = files,
    deps = heron_java_proto_files() + healthmgr_deps_files,
)

java_binary(
    name = "healthmgr-unshaded",
    srcs = [":healthmgr-main"],
    deps = heron_java_proto_files() + healthmgr_deps_files + [":healthmgr-java"],
)

genrule(
    name = "heron-healthmgr",
    srcs = [":healthmgr-unshaded_deploy.jar"],
    outs = ["heron-healthmgr.jar"],
    cmd = "cp $< $@",
)
