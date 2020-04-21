load("@rules_java//java:defs.bzl", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "aws-java-sdk",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:com_amazonaws_aws_java_sdk_core",
        "@maven//:com_amazonaws_aws_java_sdk_s3",
    ],
    deps = [
        "//third_party/java:joda_time",
        "@maven//:com_amazonaws_aws_java_sdk_core",
        "@maven//:com_amazonaws_aws_java_sdk_s3",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:commons_codec_commons_codec",
        "@maven//:commons_logging_commons_logging",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
    ],
)

java_library(
    name = "checkstyle",
    srcs = ["Empty.java"],
    exports = ["@maven//:com_puppycrawl_tools_checkstyle"],
    deps = [
        "@maven//:antlr_antlr",
        "@maven//:com_google_guava_guava",
        "@maven//:com_puppycrawl_tools_checkstyle",
        "@maven//:commons_beanutils_commons_beanutils",
        "@maven//:commons_cli_commons_cli",
        "@maven//:commons_collections_commons_collections",
        "@maven//:commons_logging_commons_logging",
    ],
)

java_library(
    name = "cli",
    srcs = ["Empty.java"],
    exports = ["@maven//:commons_cli_commons_cli"],
    deps = ["@maven//:commons_cli_commons_cli"],
)

java_library(
    name = "guava",
    srcs = ["Empty.java"],
    exports = ["@maven//:com_google_guava_guava"],
    deps = ["@maven//:com_google_guava_guava"],
)

java_library(
    name = "jackson",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
    ],
    deps = [
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
    ],
)

java_library(
    name = "jarjar",
    srcs = ["Empty.java"],
    exports = ["@maven//:org_sonatype_plugins_jarjar_maven_plugin"],
    deps = [
        "@maven//:org_ow2_asm_asm_all",
        "@maven//:org_sonatype_plugins_jarjar_maven_plugin",
    ],
)

java_library(
    name = "logging",
    srcs = ["Empty.java"],
    exports = ["@maven//:org_slf4j_slf4j_api"],
    deps = [
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_slf4j_slf4j_jdk14",
    ],
)

java_library(
    name = "kryo",
    srcs = ["Empty.java"],
    exports = ["@maven//:com_esotericsoftware_kryo"],
    deps = [
        "@maven//:com_esotericsoftware_kryo",
        "@maven//:com_esotericsoftware_minlog",
        "@maven//:com_esotericsoftware_reflectasm",
        "@maven//:org_objenesis_objenesis",
        "@maven//:org_ow2_asm_asm",
    ],
)

# This version is needed for dependents that don't want
# kryo to be included in the generated binary
# (e.g. //storm-compatibility/src/java:heron-storm)
java_library(
    name = "kryo-neverlink",
    srcs = ["Empty.java"],
    neverlink = 1,
    exports = ["@maven//:com_esotericsoftware_kryo"],
    deps = [
        "@maven//:com_esotericsoftware_kryo",
    ],
)

java_library(
    name = "yarn",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_reef_reef_common",
        "@maven//:org_apache_reef_reef_runtime_yarn",
        "@maven//:org_apache_reef_reef_utils",
        "@maven//:org_apache_reef_tang",
        "@maven//:org_apache_reef_wake",
        "@maven//:org_glassfish_hk2_external_javax_inject",
    ],
    deps = [
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_reef_reef_common",
        "@maven//:org_apache_reef_reef_runtime_yarn",
        "@maven//:org_apache_reef_reef_utils",
        "@maven//:org_apache_reef_tang",
        "@maven//:org_apache_reef_wake",
        "@maven//:org_glassfish_hk2_external_javax_inject",
    ],
)

java_library(
    name = "hadoop-core",
    srcs = ["Empty.java"],
    neverlink = 1,
    exports = ["@maven//:org_apache_hadoop_hadoop_core"],
    deps = [
        "@maven//:org_apache_hadoop_hadoop_core",
    ],
)

java_library(
    name = "powermock",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:org_powermock_powermock_api_mockito",
        "@maven//:org_powermock_powermock_api_support",
        "@maven//:org_powermock_powermock_core",
        "@maven//:org_powermock_powermock_module_junit4",
        "@maven//:org_powermock_powermock_module_junit4_common",
    ],
    deps = [
        "@maven//:org_javassist_javassist",
        "@maven//:org_powermock_powermock_api_mockito",
        "@maven//:org_powermock_powermock_api_support",
        "@maven//:org_powermock_powermock_core",
        "@maven//:org_powermock_powermock_module_junit4",
        "@maven//:org_powermock_powermock_module_junit4_common",
        "@maven//:org_powermock_powermock_reflect",
    ],
)

# This version is needed for dependents that don't want
# protobuf to be included in the generated binary
java_library(
    name = "protobuf-java-neverlink",
    srcs = ["Empty.java"],
    neverlink = 1,
    exports = ["@maven//:com_puppycrawl_tools_checkstyle"],
    deps = ["@com_google_protobuf//:protobuf_java"],
)

java_library(
    name = "joda_time",
    srcs = ["Empty.java"],
    exports = ["@maven//:joda_time_joda_time"],
    deps = ["@maven//:joda_time_joda_time"],
)

java_library(
    name = "junit4",
    srcs = ["Empty.java"],
    exports = ["@maven//:junit_junit"],
    deps = ["@maven//:junit_junit"],
)

java_library(
    name = "mockito",
    srcs = ["Empty.java"],
    exports = ["@maven//:org_mockito_mockito_all"],
    deps = ["@maven//:org_mockito_mockito_all"],
)

java_library(
    name = "google-api-services-storage",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:com_google_api_client_google_api_client",
        "@maven//:com_google_apis_google_api_services_storage",
        "@maven//:com_google_http_client_google_http_client",
        "@maven//:com_google_http_client_google_http_client_jackson2",
        "@maven//:com_google_oauth_client_google_oauth_client",
    ],
    deps = [
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_google_api_client_google_api_client",
        "@maven//:com_google_http_client_google_http_client",
        "@maven//:com_google_http_client_google_http_client_jackson2",
        "@maven//:com_google_oauth_client_google_oauth_client",
        "@maven//:commons_codec_commons_codec",
        "@maven//:commons_logging_commons_logging",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
    ],
)

java_library(
    name = "apache-pulsar-client",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:org_apache_pulsar_pulsar_client_shaded",
    ],
    deps = [
        "@maven//:org_apache_pulsar_pulsar_client_shaded",
    ],
)

java_library(
    name = "kubernetes-java-client",
    srcs = ["Empty.java"],
    exports = [
        "//third_party/java:joda_time",
        "@maven//:com_google_code_gson_gson",
        "@maven//:com_squareup_okhttp3_logging_interceptor",
        "@maven//:com_squareup_okhttp3_okhttp",
        "@maven//:com_squareup_okio_okio",
        "@maven//:commons_codec_commons_codec",
        "@maven//:commons_lang_commons_lang",
        "@maven//:io_gsonfire_gson_fire",
        "@maven//:io_kubernetes_client_java",
        "@maven//:io_kubernetes_client_java_api",
        "@maven//:io_kubernetes_client_java_proto",
        "@maven//:io_swagger_swagger_annotations",
        "@maven//:log4j_log4j",
        "@maven//:org_yaml_snakeyaml",
    ],
    deps = [
        "//third_party/java:joda_time",
        "@maven//:com_google_code_gson_gson",
        "@maven//:com_squareup_okhttp3_logging_interceptor",
        "@maven//:com_squareup_okhttp3_okhttp",
        "@maven//:com_squareup_okio_okio",
        "@maven//:commons_codec_commons_codec",
        "@maven//:commons_lang_commons_lang",
        "@maven//:io_gsonfire_gson_fire",
        "@maven//:io_kubernetes_client_java",
        "@maven//:io_kubernetes_client_java_api",
        "@maven//:io_kubernetes_client_java_proto",
        "@maven//:io_swagger_swagger_annotations",
        "@maven//:log4j_log4j",
        "@maven//:org_yaml_snakeyaml",
    ],
)

java_library(
    name = "commons-compress",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:org_apache_commons_commons_compress",
    ],
    deps = [
        "@maven//:org_apache_commons_commons_compress",
    ],
)

java_library(
    name = "jetty-jersey-java",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:javax_annotation_javax_annotation_api",
        "@maven//:javax_servlet_javax_servlet_api",
        "@maven//:javax_validation_validation_api",
        "@maven//:javax_ws_rs_javax_ws_rs_api",
        "@maven//:org_eclipse_jetty_jetty_continuation",
        "@maven//:org_eclipse_jetty_jetty_http",
        "@maven//:org_eclipse_jetty_jetty_io",
        "@maven//:org_eclipse_jetty_jetty_security",
        "@maven//:org_eclipse_jetty_jetty_server",
        "@maven//:org_eclipse_jetty_jetty_servlet",
        "@maven//:org_eclipse_jetty_jetty_servlets",
        "@maven//:org_eclipse_jetty_jetty_util",
        "@maven//:org_glassfish_hk2_external_aopalliance_repackaged",
        "@maven//:org_glassfish_hk2_external_javax_inject",
        "@maven//:org_glassfish_hk2_hk2_api",
        "@maven//:org_glassfish_hk2_hk2_locator",
        "@maven//:org_glassfish_hk2_hk2_utils",
        "@maven//:org_glassfish_hk2_osgi_resource_locator",
        "@maven//:org_glassfish_jersey_bundles_repackaged_jersey_guava",
        "@maven//:org_glassfish_jersey_containers_jersey_container_servlet",
        "@maven//:org_glassfish_jersey_containers_jersey_container_servlet_core",
        "@maven//:org_glassfish_jersey_core_jersey_client",
        "@maven//:org_glassfish_jersey_core_jersey_common",
        "@maven//:org_glassfish_jersey_core_jersey_server",
        "@maven//:org_glassfish_jersey_media_jersey_media_jaxb",
        "@maven//:org_glassfish_jersey_media_jersey_media_multipart",
        "@maven//:org_javassist_javassist",
        "@maven//:org_jvnet_mimepull_mimepull",
    ],
    deps = [
        "@maven//:javax_annotation_javax_annotation_api",
        "@maven//:javax_servlet_javax_servlet_api",
        "@maven//:javax_validation_validation_api",
        "@maven//:javax_ws_rs_javax_ws_rs_api",
        "@maven//:org_eclipse_jetty_jetty_continuation",
        "@maven//:org_eclipse_jetty_jetty_http",
        "@maven//:org_eclipse_jetty_jetty_io",
        "@maven//:org_eclipse_jetty_jetty_security",
        "@maven//:org_eclipse_jetty_jetty_server",
        "@maven//:org_eclipse_jetty_jetty_servlet",
        "@maven//:org_eclipse_jetty_jetty_servlets",
        "@maven//:org_eclipse_jetty_jetty_util",
        "@maven//:org_glassfish_hk2_external_aopalliance_repackaged",
        "@maven//:org_glassfish_hk2_external_javax_inject",
        "@maven//:org_glassfish_hk2_hk2_api",
        "@maven//:org_glassfish_hk2_hk2_locator",
        "@maven//:org_glassfish_hk2_hk2_utils",
        "@maven//:org_glassfish_hk2_osgi_resource_locator",
        "@maven//:org_glassfish_jersey_bundles_repackaged_jersey_guava",
        "@maven//:org_glassfish_jersey_containers_jersey_container_servlet",
        "@maven//:org_glassfish_jersey_containers_jersey_container_servlet_core",
        "@maven//:org_glassfish_jersey_core_jersey_client",
        "@maven//:org_glassfish_jersey_core_jersey_common",
        "@maven//:org_glassfish_jersey_core_jersey_server",
        "@maven//:org_glassfish_jersey_media_jersey_media_jaxb",
        "@maven//:org_glassfish_jersey_media_jersey_media_multipart",
        "@maven//:org_javassist_javassist",
        "@maven//:org_jvnet_mimepull_mimepull",
    ],
)

java_library(
    name = "dlog-java",
    srcs = ["Empty.java"],
    exports = [
        "@maven//:io_netty_netty_all",
        "@maven//:org_apache_distributedlog_distributedlog_core_shaded",
    ],
    deps = [
        "@maven//:commons_collections_commons_collections",
        "@maven//:commons_configuration_commons_configuration",
        "@maven//:commons_lang_commons_lang",
        "@maven//:commons_logging_commons_logging",
        "@maven//:io_netty_netty_all",
        "@maven//:org_apache_distributedlog_distributedlog_core_shaded",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_slf4j_slf4j_jdk14",
    ],
)
