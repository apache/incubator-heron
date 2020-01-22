licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "aws-java-sdk",
    srcs = ["Empty.java"],
    exports = [
        "@com_amazonaws_aws_java_sdk_core//jar",
        "@com_amazonaws_aws_java_sdk_s3//jar",
    ],
    deps = [
        "//third_party/java:joda_time",
        "@com_amazonaws_aws_java_sdk_core//jar",
        "@com_amazonaws_aws_java_sdk_s3//jar",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@commons_codec//jar",
        "@commons_logging_commons_logging//jar",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
    ],
)

java_library(
    name = "checkstyle",
    srcs = ["Empty.java"],
    exports = ["@com_puppycrawl_tools_checkstyle//jar"],
    deps = [
        "@antlr_antlr//jar",
        "@com_google_guava_guava//jar",
        "@com_puppycrawl_tools_checkstyle//jar",
        "@commons_beanutils_commons_beanutils//jar",
        "@commons_cli_commons_cli//jar",
        "@commons_collections_commons_collections//jar",
        "@commons_logging_commons_logging//jar",
    ],
)

java_library(
    name = "cli",
    srcs = ["Empty.java"],
    exports = ["@commons_cli_commons_cli//jar"],
    deps = ["@commons_cli_commons_cli//jar"],
)

java_library(
    name = "guava",
    srcs = ["Empty.java"],
    exports = ["@com_google_guava_guava//jar"],
    deps = ["@com_google_guava_guava//jar"],
)

java_library(
    name = "jackson",
    srcs = ["Empty.java"],
    exports = [
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
    ],
    deps = [
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
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
    exports = ["@com_esotericsoftware_kryo//jar"],
    deps = [
        "@com_esotericsoftware_kryo//jar",
        "@com_esotericsoftware_minlog//jar",
        "@com_esotericsoftware_reflectasm//jar",
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
    exports = ["@com_esotericsoftware_kryo//jar"],
    deps = [
        "@com_esotericsoftware_kryo//jar",
    ],
)

java_library(
    name = "yarn",
    srcs = ["Empty.java"],
    exports = [
        "@javax_inject_javax_inject//jar",
        "@maven//:org_apache_reef_reef_common",
        "@maven//:org_apache_reef_reef_runtime_yarn",
        "@maven//:org_apache_reef_reef_utils",
        "@maven//:org_apache_reef_tang",
        "@maven//:org_apache_reef_wake",
        "@org_apache_commons_commons_lang3//jar",
    ],
    deps = [
        "@javax_inject_javax_inject//jar",
        "@maven//:org_apache_reef_reef_common",
        "@maven//:org_apache_reef_reef_runtime_yarn",
        "@maven//:org_apache_reef_reef_utils",
        "@maven//:org_apache_reef_tang",
        "@maven//:org_apache_reef_wake",
        "@org_apache_commons_commons_lang3//jar",
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
    exports = ["@com_puppycrawl_tools_checkstyle//jar"],
    deps = ["@com_google_protobuf//:protobuf_java"],
)

java_library(
    name = "joda_time",
    srcs = ["Empty.java"],
    exports = ["@joda_time_joda_time//jar"],
    deps = ["@joda_time_joda_time//jar"],
)

java_library(
    name = "junit4",
    srcs = ["Empty.java"],
    exports = ["@junit_junit//jar"],
    deps = ["@junit_junit//jar"],
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
        "@google_api_client//jar",
        "@google_api_services_storage//jar",
        "@google_http_client//jar",
        "@google_http_client_jackson2//jar",
        "@google_oauth_client//jar",
    ],
    deps = [
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@commons_codec//jar",
        "@commons_logging_commons_logging//jar",
        "@google_api_client//jar",
        "@google_http_client//jar",
        "@google_http_client_jackson2//jar",
        "@google_oauth_client//jar",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
    ],
)

java_library(
    name = "apache-pulsar-client",
    srcs = ["Empty.java"],
    exports = [
        "@apache_pulsar_client//jar",
    ],
    deps = [
        "@apache_pulsar_client//jar",
    ],
)

java_library(
    name = "kubernetes-java-client",
    srcs = ["Empty.java"],
    exports = [
        "//third_party/java:joda_time",
        "@commons_codec//jar",
        "@commons_lang_commons_lang//jar",
        "@google_gson//jar",
        "@io_gsonfire//jar",
        "@kubernetes_java_client//jar",
        "@kubernetes_java_client_api//jar",
        "@kubernetes_java_client_proto//jar",
        "@log4j_log4j//jar",
        "@maven//:com_squareup_okhttp3_logging_interceptor",
        "@maven//:com_squareup_okhttp3_okhttp",
        "@maven//:com_squareup_okio_okio",
        "@maven//:io_swagger_swagger_annotations",
        "@maven//:org_yaml_snakeyaml",
    ],
    deps = [
        "//third_party/java:joda_time",
        "@commons_codec//jar",
        "@commons_lang_commons_lang//jar",
        "@google_gson//jar",
        "@io_gsonfire//jar",
        "@kubernetes_java_client//jar",
        "@kubernetes_java_client_api//jar",
        "@kubernetes_java_client_proto//jar",
        "@log4j_log4j//jar",
        "@maven//:com_squareup_okhttp3_logging_interceptor",
        "@maven//:com_squareup_okhttp3_okhttp",
        "@maven//:com_squareup_okio_okio",
        "@maven//:io_swagger_swagger_annotations",
        "@maven//:org_yaml_snakeyaml",
    ],
)

java_library(
    name = "commons-compress",
    srcs = ["Empty.java"],
    exports = [
        "@org_apache_commons_compress//jar",
    ],
    deps = [
        "@org_apache_commons_compress//jar",
    ],
)

java_library(
    name = "jetty-jersey-java",
    srcs = ["Empty.java"],
    exports = [
        "@hk2_aopalliance_repackaged//jar",
        "@hk2_api//jar",
        "@hk2_locator//jar",
        "@hk2_osgi_resource_locator//jar",
        "@hk2_utils//jar",
        "@javax_annotation//jar",
        "@javax_inject//jar",
        "@javax_servlet_api//jar",
        "@javax_validation//jar",
        "@javax_ws_rs_api//jar",
        "@jersey_client//jar",
        "@jersey_common//jar",
        "@jersey_container_servlet//jar",
        "@jersey_container_servlet_core//jar",
        "@jersey_guava//jar",
        "@jersey_media_jaxb//jar",
        "@jersey_media_multipart//jar",
        "@jersey_server//jar",
        "@maven//:org_eclipse_jetty_jetty_continuation",
        "@maven//:org_eclipse_jetty_jetty_http",
        "@maven//:org_eclipse_jetty_jetty_io",
        "@maven//:org_eclipse_jetty_jetty_security",
        "@maven//:org_eclipse_jetty_jetty_server",
        "@maven//:org_eclipse_jetty_jetty_servlet",
        "@maven//:org_eclipse_jetty_jetty_servlets",
        "@maven//:org_eclipse_jetty_jetty_util",
        "@maven//:org_javassist_javassist",
        "@mimepull//jar",
    ],
    deps = [
        "@hk2_aopalliance_repackaged//jar",
        "@hk2_api//jar",
        "@hk2_locator//jar",
        "@hk2_osgi_resource_locator//jar",
        "@hk2_utils//jar",
        "@javax_annotation//jar",
        "@javax_inject//jar",
        "@javax_servlet_api//jar",
        "@javax_validation//jar",
        "@javax_ws_rs_api//jar",
        "@jersey_client//jar",
        "@jersey_common//jar",
        "@jersey_container_servlet//jar",
        "@jersey_container_servlet_core//jar",
        "@jersey_guava//jar",
        "@jersey_media_jaxb//jar",
        "@jersey_media_multipart//jar",
        "@jersey_server//jar",
        "@maven//:org_eclipse_jetty_jetty_continuation",
        "@maven//:org_eclipse_jetty_jetty_http",
        "@maven//:org_eclipse_jetty_jetty_io",
        "@maven//:org_eclipse_jetty_jetty_security",
        "@maven//:org_eclipse_jetty_jetty_server",
        "@maven//:org_eclipse_jetty_jetty_servlet",
        "@maven//:org_eclipse_jetty_jetty_servlets",
        "@maven//:org_eclipse_jetty_jetty_util",
        "@maven//:org_javassist_javassist",
        "@mimepull//jar",
    ],
)

java_library(
    name = "dlog-java",
    srcs = ["Empty.java"],
    exports = [
        "@io_netty_netty_all//jar",
        "@maven//:org_apache_distributedlog_distributedlog_core_shaded",
    ],
    deps = [
        "@commons_collections_commons_collections//jar",
        "@commons_configuration_commons_configuration//jar",
        "@commons_lang_commons_lang//jar",
        "@commons_logging_commons_logging//jar",
        "@io_netty_netty_all//jar",
        "@maven//:org_apache_distributedlog_distributedlog_core_shaded",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_slf4j_slf4j_jdk14",
    ],
)
