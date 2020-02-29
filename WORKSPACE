#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

workspace(name = "org_apache_heron")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")

# versions shared across artifacts that should be upgraded together
aws_version = "1.11.58"
curator_version = "2.9.0"
google_client_version = "1.22.0"
jackson_version = "2.8.8"
powermock_version = "1.6.2"
reef_version = "0.14.0"
slf4j_version = "1.7.7"
distributedlog_version = "4.7.3"
http_client_version = "4.5.2"

# heron API server
jetty_version = "9.4.6.v20170531"
jersey_verion = "2.25.1"
hk2_api = "2.5.0-b32"

maven_server(
  name = "default",
  url = "https://repo1.maven.org/maven2/",
)

maven_server(
  name = "maven_twttr_com",
  url = "https://maven.twttr.com",
)

maven_jar(
  name = "org_apache_avro_avro",
  artifact = "org.apache.avro:avro:1.7.4"
)

maven_jar(
  name = "antlr_antlr",
  artifact = "antlr:antlr:2.7.7",
)

maven_jar(
    name = "aopalliance_aopalliance",
    artifact = "aopalliance:aopalliance:1.0",
)

maven_jar(
  name = "org_ow2_asm_asm_all",
  artifact = "org.ow2.asm:asm-all:5.1",
)

maven_jar(
  name = "com_amazonaws_aws_java_sdk_core",
  artifact = "com.amazonaws:aws-java-sdk-core:" + aws_version,
)

maven_jar(
  name = "com_amazonaws_aws_java_sdk_s3",
  artifact = "com.amazonaws:aws-java-sdk-s3:" + aws_version,
)

maven_jar(
  name = "com_puppycrawl_tools_checkstyle",
  artifact = "com.puppycrawl.tools:checkstyle:6.17",
)

maven_jar(
  name = "commons_beanutils_commons_beanutils",
  artifact = "commons-beanutils:commons-beanutils:1.9.2",
)

maven_jar(
  name = "commons_codec",
  artifact = "commons-codec:commons-codec:1.9",
)

maven_jar(
  name = "commons_io_commons_io",
  artifact = "commons-io:commons-io:2.4",
)

maven_jar(
  name = "commons_configuration_commons_configuration",
  artifact = "commons-configuration:commons-configuration:1.6",
)

maven_jar(
  name = "commons_cli_commons_cli",
  artifact = "commons-cli:commons-cli:1.3.1",
)

maven_jar(
  name = "commons_collections_commons_collections",
  artifact = "commons-collections:commons-collections:3.2.1",
)

maven_jar(
  name = "org_apache_commons_commons_lang3",
  artifact = "org.apache.commons:commons-lang3:3.3.2",
)

maven_jar(
  name = "commons_lang_commons_lang",
  artifact = "commons-lang:commons-lang:2.6",
)

maven_jar(
  name = "commons_logging_commons_logging",
  artifact = "commons-logging:commons-logging:1.1.1",
)

maven_jar(
  name = "org_apache_curator_curator_client",
  artifact = "org.apache.curator:curator-client:" + curator_version,
)

maven_jar(
  name = "org_apache_curator_curator_framework",
  artifact = "org.apache.curator:curator-framework:" + curator_version,
)

maven_jar(
  name = "org_apache_curator_curator_recipes",
  artifact = "org.apache.curator:curator-recipes:" + curator_version,
)

maven_jar(
  name = "org_apache_curator_curator_test",
  artifact = "org.apache.curator:curator-test:" + curator_version,
)

maven_jar(
  name = "com_google_guava_guava",
  artifact = "com.google.guava:guava:18.0",
)

maven_jar(
    name = "com_google_inject_guice",
    artifact = "com.google.inject:guice:4.0",
)

maven_jar(
    name = "com_google_inject_extensions_guice_assistedinject",
    artifact = "com.google.inject.extensions:guice-assistedinject:4.0",
)

maven_jar(
  name = "org_apache_hadoop_hadoop_core",
  artifact = "org.apache.hadoop:hadoop-core:0.20.2",
)

maven_jar(
  name = "org_apache_httpcomponents_httpmime",
  artifact = "org.apache.httpcomponents:httpmime:4.4",
)

maven_jar(
  name = "org_apache_httpcomponents_http_client",
  artifact = "org.apache.httpcomponents:httpclient:" + http_client_version,
)

http_jar(
  name = "org_apache_httpcomponents_http_client_test",
  url = "https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/" +
  http_client_version + "/httpclient-" + http_client_version + "-tests.jar"
)

maven_jar(
  name = "org_apache_httpcomponents_http_core",
  artifact = "org.apache.httpcomponents:httpcore:4.4.5",
)

maven_jar(
  name = "com_jayway_jsonpath",
  artifact = "com.jayway.jsonpath:json-path:2.1.0",
)

maven_jar(
  name = "com_fasterxml_jackson_core_jackson_annotations",
  artifact = "com.fasterxml.jackson.core:jackson-annotations:" + jackson_version,
)

maven_jar(
  name = "com_fasterxml_jackson_core_jackson_core",
  artifact = "com.fasterxml.jackson.core:jackson-core:" + jackson_version,
)

maven_jar(
  name = "com_fasterxml_jackson_core_jackson_databind",
  artifact = "com.fasterxml.jackson.core:jackson-databind:" + jackson_version,
)

maven_jar(
  name = "net_minidev_json_smart",
  artifact = "net.minidev:json-smart:2.2"
)

maven_jar(
  name = "org_codehaus_jackson_jackson_core_asl",
  artifact = "org.codehaus.jackson:jackson-core-asl:1.9.13",
)

maven_jar(
  name = "org_codehaus_jackson_jackson_mapper_asl",
  artifact = "org.codehaus.jackson:jackson-mapper-asl:1.9.13",
)

maven_jar(
  name = "org_javassist_javassist",
  artifact = "org.javassist:javassist:3.18.1-GA",
)

maven_jar(
  name = "javax_inject_javax_inject",
  artifact = "javax.inject:javax.inject:1",
)

maven_jar(
  name = "javax_ws_rs_javax_ws_rs_api",
  artifact = "javax.ws.rs:javax.ws.rs-api:2.0.1",
)

maven_jar(
   name = "org_glassfish_jersey_core_jersey_client",
   artifact = "org.glassfish.jersey.core:jersey-client:2.24",
)

maven_jar(
   name = "org_glassfish_hk2_hk2_api",
   artifact = "org.glassfish.hk2:hk2-api:2.5.0-b05",
)

maven_jar(
   name = "org_glassfish_jersey_ext_jersey_entity_filtering",
   artifact = "org.glassfish.jersey.ext:jersey-entity-filtering:2.24",
)

maven_jar(
   name = "org_apache_samoa_samoa_storm",
   artifact = "org.apache.samoa:samoa-storm:0.4.0-incubating",
)

maven_jar(
   name = "javax_annotation_javax_annotation_api",
   artifact = "javax.annotation:javax.annotation-api:1.3.2",
)

maven_jar(
   name = "com_fasterxml_jackson_module_jackson_module_jaxb_annotations",
   artifact = "com.fasterxml.jackson.module:jackson-module-jaxb-annotations:2.5.4",
)

maven_jar(
   name = "com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider",
   artifact = "com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:2.5.4",
)

maven_jar(
   name = "com_fasterxml_jackson_jaxrs_jackson_jaxrs_base",
   artifact = "com.fasterxml.jackson.jaxrs:jackson-jaxrs-base:2.5.4",
)

maven_jar(
   name = "org_glassfish_hk2_hk2_locator",
   artifact = "org.glassfish.hk2:hk2-locator:2.5.0-b05",
)

maven_jar(
   name = "org_glassfish_hk2_hk2_utils",
   artifact = "org.glassfish.hk2:hk2-utils:2.5.0-b05",
)

maven_jar(
   name = "org_glassfish_jersey_bundles_repackaged_jersey_guava",
   artifact = "org.glassfish.jersey.bundles.repackaged:jersey-guava:2.24",
)

maven_jar(
   name = "org_glassfish_jersey_core_jersey_common",
   artifact = "org.glassfish.jersey.core:jersey-common:2.24",
)

maven_jar(
   name = "org_glassfish_jersey_media_jersey_media_json_jackson",
   artifact = "org.glassfish.jersey.media:jersey-media-json-jackson:2.24",
)

maven_jar(
  name = "org_sonatype_plugins_jarjar_maven_plugin",
  artifact = "org.sonatype.plugins:jarjar-maven-plugin:1.9",
)

maven_jar(
  name = "com_googlecode_json_simple_json_simple",
  artifact = "com.googlecode.json-simple:json-simple:1.1",
)

maven_jar(
  name = "com_esotericsoftware_kryo",
  artifact = "com.esotericsoftware:kryo:3.0.3",
)

maven_jar(
  name = "com_esotericsoftware_reflectasm",
  artifact = "com.esotericsoftware:reflectasm:1.11.3",
)

maven_jar(
  name = "org_objectweb_asm",
  artifact = "org.ow2.asm:asm:5.0.4",
)

maven_jar(
  name = "org_apache_mesos_mesos",
  artifact = "org.apache.mesos:mesos:0.22.0",
)

maven_jar(
  name = "com_esotericsoftware_minlog",
  artifact = "com.esotericsoftware:minlog:1.3.0",
)

maven_jar(
  name = "io_netty_netty_all",
  artifact = "io.netty:netty-all:4.1.22.Final"
)

maven_jar(
  name = "org_objenesis_objenesis",
  artifact = "org.objenesis:objenesis:2.1",
)

maven_jar(
  name = "org_powermock_powermock_api_mockito",
  artifact = "org.powermock:powermock-api-mockito:" + powermock_version,
)

maven_jar(
  name = "org_powermock_powermock_api_support",
  artifact = "org.powermock:powermock-api-support:" + powermock_version,
)

maven_jar(
  name = "org_powermock_powermock_core",
  artifact = "org.powermock:powermock-core:" + powermock_version,
)

maven_jar(
  name = "org_powermock_powermock_module_junit4",
  artifact = "org.powermock:powermock-module-junit4:" + powermock_version,
)

maven_jar(
  name = "org_powermock_powermock_module_junit4_common",
  artifact = "org.powermock:powermock-module-junit4-common:" + powermock_version,
)

maven_jar(
  name = "org_powermock_powermock_reflect",
  artifact = "org.powermock:powermock-reflect:" + powermock_version,
)

maven_jar(
  name = "org_apache_reef_reef_common",
  artifact = "org.apache.reef:reef-common:" + reef_version
)

maven_jar(
  name = "org_apache_reef_reef_runtime_local",
  artifact = "org.apache.reef:reef-runtime-local:" + reef_version
)

maven_jar(
  name = "org_apache_reef_reef_runtime_yarn",
  artifact = "org.apache.reef:reef-runtime-yarn:" + reef_version
)

maven_jar(
  name = "org_apache_reef_reef_utils",
  artifact = "org.apache.reef:reef-utils:" + reef_version
)

maven_jar(
  name = "org_apache_reef_tang",
  artifact = "org.apache.reef:tang:" + reef_version
)

maven_jar(
  name = "org_slf4j_slf4j_api",
  artifact = "org.slf4j:slf4j-api:" + slf4j_version
)

maven_jar(
  name = "org_slf4j_slf4j_jdk14",
  artifact = "org.slf4j:slf4j-jdk14:" + slf4j_version
)

maven_jar(
  name = "org_yaml_snakeyaml",
  artifact = "org.yaml:snakeyaml:1.15",
)

maven_jar(
  name = "org_apache_thrift_libthrift",
  artifact = "org.apache.thrift:libthrift:0.5.0-1",
  server = "maven_twttr_com",
)

maven_jar(
  name = "org_apache_reef_wake",
  artifact = "org.apache.reef:wake:" + reef_version
)

maven_jar(
  name = "org_apache_zookeeper_zookeeper",
  artifact = "org.apache.zookeeper:zookeeper:3.4.14",
)

maven_jar(
  name = "joda_time_joda_time",
  artifact = "joda-time:joda-time:2.3",
)

maven_jar(
  name = "junit_junit",
  artifact = "junit:junit:4.11",
)

maven_jar(
  name = "org_mockito_mockito_all",
  artifact = "org.mockito:mockito-all:1.10.19",
)

maven_jar(
  name = "org_apache_kafka_kafka_210",
  artifact = "org.apache.kafka:kafka_2.10:0.8.2.1",
)

maven_jar(
  name = "org_apache_kafka_kafka_clients",
  artifact = "org.apache.kafka:kafka-clients:2.2.0",
)

maven_jar(
  name = "org_scala_lang_scala_library",
  artifact = "org.scala-lang:scala-library:2.10.3",
)

maven_jar(
  name = "log4j_log4j",
  artifact = "log4j:log4j:1.2.17",
)

maven_jar(
  name = "com_yammer_metrics_metrics_core",
  artifact = "com.yammer.metrics:metrics-core:2.2.0",
)

maven_jar(
  name = "com_101tec_zkclient",
  artifact = "com.101tec:zkclient:0.3"
)

maven_jar(
  name = "com_microsoft_dhalion",
  artifact = "com.microsoft.dhalion:dhalion:0.2.3",
)

maven_jar(
  name = "org_apache_commons_commons_math3",
  artifact = "org.apache.commons:commons-math3:3.6.1"
)

maven_jar(
  name = "tech_tablesaw",
  artifact = "tech.tablesaw:tablesaw-core:0.11.4"
)

maven_jar(
  name = "it_unimi_dsi_fastutil",
  artifact = "it.unimi.dsi:fastutil:8.1.1"
)

maven_jar(
  name = "org_roaringbitmap",
  artifact = "org.roaringbitmap:RoaringBitmap:0.6.51"
)

# Google Cloud
maven_jar(
  name = "google_api_services_storage",
  artifact = "com.google.apis:google-api-services-storage:v1-rev108-" + google_client_version
)

maven_jar(
  name = "google_api_client",
  artifact = "com.google.api-client:google-api-client:" + google_client_version
)

maven_jar(
  name = "google_http_client",
  artifact = "com.google.http-client:google-http-client:" + google_client_version
)

maven_jar(
  name = "google_http_client_jackson2",
  artifact = "com.google.http-client:google-http-client-jackson2:" + google_client_version
)

maven_jar(
  name = "google_oauth_client",
  artifact = "com.google.oauth-client:google-oauth-client:" + google_client_version
)
# end Google Cloud

# Pulsar Client
maven_jar(
  name = "apache_pulsar_client",
  artifact = "org.apache.pulsar:pulsar-client:jar:shaded:1.19.0-incubating"
)
# end Pulsar Client

# Kubernetes java client
kubernetes_client_version = "7.0.0"
squareup_okhttp_version = "3.14.5"

maven_jar(
  name = "kubernetes_java_client",
  artifact = "io.kubernetes:client-java:" + kubernetes_client_version
)

maven_jar(
  name = "kubernetes_java_client_api",
  artifact = "io.kubernetes:client-java-api:" + kubernetes_client_version
)

maven_jar(
  name = "swagger_annotations",
  artifact = "io.swagger:swagger-annotations:1.5.12"
)

maven_jar(
  name = "squareup_okhttp",
  artifact = "com.squareup.okhttp3:okhttp:" + squareup_okhttp_version
)
maven_jar(
  name = "squareup_okio",
  artifact = "com.squareup.okio:okio:1.17.2"
)
maven_jar(
  name = "squareup_okhttp_logging_interceptor",
  artifact = "com.squareup.okhttp3:logging-interceptor:" + squareup_okhttp_version
)

maven_jar(
  name = "google_gson",
  artifact = "com.google.code.gson:gson:2.8.0"
)

maven_jar(
  name = "io_gsonfire",
  artifact = "io.gsonfire:gson-fire:1.8.3"
)

maven_jar(
  name = "kubernetes_java_client_proto",
  artifact = "io.kubernetes:client-java-proto:" + kubernetes_client_version
)

# end Kubernetes java client

# heron API server
# jetty
maven_jar(
  name = "org_eclipse_jetty_server",
  artifact = "org.eclipse.jetty:jetty-server:" + jetty_version
)

maven_jar(
  name = "org_eclipse_jetty_http",
  artifact = "org.eclipse.jetty:jetty-http:" + jetty_version
)

maven_jar(
  name = "org_eclipse_jetty_util",
  artifact = "org.eclipse.jetty:jetty-util:" + jetty_version
)

maven_jar(
  name = "org_eclipse_jetty_io",
  artifact = "org.eclipse.jetty:jetty-io:" + jetty_version
)

maven_jar(
  name = "org_eclipse_jetty_security",
  artifact = "org.eclipse.jetty:jetty-security:" + jetty_version
)

maven_jar(
  name = "org_eclipse_jetty_servlet",
  artifact = "org.eclipse.jetty:jetty-servlet:" + jetty_version
)

maven_jar(
  name = "org_eclipse_jetty_servlets",
  artifact = "org.eclipse.jetty:jetty-servlets:" + jetty_version
)

maven_jar(
  name = "org_eclipse_jetty_continuation",
  artifact = "org.eclipse.jetty:jetty-continuation:" + jetty_version
)

maven_jar(
  name = "javax_servlet_api",
  artifact = "javax.servlet:javax.servlet-api:3.1.0"
)
# end jetty

# jersey
maven_jar(
  name = "jersey_container_servlet_core",
  artifact = "org.glassfish.jersey.containers:jersey-container-servlet-core:" + jersey_verion
)

maven_jar(
  name = "jersey_container_servlet",
  artifact = "org.glassfish.jersey.containers:jersey-container-servlet:" + jersey_verion
)

maven_jar(
  name = "jersey_server",
  artifact = "org.glassfish.jersey.core:jersey-server:" + jersey_verion
)

maven_jar(
  name = "jersey_client",
  artifact = "org.glassfish.jersey.core:jersey-client:" + jersey_verion
)

maven_jar(
  name = "jersey_common",
  artifact = "org.glassfish.jersey.core:jersey-common:jar:" + jersey_verion
)

maven_jar(
  name = "jersey_media_multipart",
  artifact = "org.glassfish.jersey.media:jersey-media-multipart:" + jersey_verion
)

maven_jar(
  name = "jersey_media_jaxb",
  artifact = "org.glassfish.jersey.media:jersey-media-jaxb:" + jersey_verion
)

maven_jar(
  name = "jersey_guava",
  artifact = "org.glassfish.jersey.bundles.repackaged:jersey-guava:" + jersey_verion
)
# end jersey

maven_jar(
  name = "javax_inject",
  artifact = "org.glassfish.hk2.external:javax.inject:2.5.0-b32"
)

maven_jar(
  name = "javax_annotation",
  artifact = "javax.annotation:javax.annotation-api:1.2"
)

maven_jar(
  name = "javax_validation",
  artifact = "javax.validation:validation-api:1.1.0.Final"
)

maven_jar(
  name = "javax_ws_rs_api",
  artifact = "javax.ws.rs:javax.ws.rs-api:2.0.1"
)

maven_jar(
  name = "hk2_api",
  artifact = "org.glassfish.hk2:hk2-api:" + hk2_api
)

maven_jar(
  name = "hk2_utils",
  artifact = "org.glassfish.hk2:hk2-utils:" + hk2_api
)

maven_jar(
  name = "hk2_aopalliance_repackaged",
  artifact = "org.glassfish.hk2.external:aopalliance-repackaged:" + hk2_api
)

maven_jar(
  name = "hk2_locator",
  artifact = "org.glassfish.hk2:hk2-locator:" + hk2_api
)

maven_jar(
  name = "hk2_osgi_resource_locator",
  artifact = "org.glassfish.hk2:osgi-resource-locator:1.0.1"
)

maven_jar(
  name = "org_javassit",
  artifact = "org.javassist:javassist:3.20.0-GA"
)

maven_jar(
  name = "mimepull",
  artifact = "org.jvnet.mimepull:mimepull:1.9.7"
)

maven_jar(
  name = "org_apache_commons_compress",
  artifact = "org.apache.commons:commons-compress:1.14",
)

maven_jar(
  name = "javax_xml_bind_jaxb",
  artifact = "javax.xml.bind:jaxb-api:2.3.0",
)

maven_jar(
  name = "javax_activation_activation",
  artifact = "javax.activation:activation:1.1.1",
)


# bookkeeper & distributedlog dependencies
maven_jar(
  name = "org_apache_distributedlog_core",
  artifact = "org.apache.distributedlog:distributedlog-core-shaded:" + distributedlog_version
)
# end bookkeeper & distributedlog dependencies

# end heron API server

# Nomad dependencies
maven_jar(
  name = "com_hashicorp_nomad",
  artifact = "com.hashicorp.nomad:nomad-sdk:0.7.0"
)

# Nomad transitive dependencies
maven_jar(
      name = "com_google_code_findbugs_jsr305",
      artifact = "com.google.code.findbugs:jsr305:3.0.2",
)

maven_jar(
      name = "org_bouncycastle_bcprov_jdk15on",
      artifact = "org.bouncycastle:bcprov-jdk15on:1.56",
)

maven_jar(
      name = "org_bouncycastle_bcpkix_jdk15on",
      artifact = "org.bouncycastle:bcpkix-jdk15on:1.56",
)

maven_jar(
      name = "commons_codec_commons_codec",
      artifact = "commons-codec:commons-codec:1.9",
      sha1 = "9ce04e34240f674bc72680f8b843b1457383161a",
  )

# End Nomand dependencies

# for pex repos
PEX_SRC = "https://pypi.python.org/packages/3a/1d/cd41cd3765b78a4353bbf27d18b099f7afbcd13e7f2dc9520f304ec8981c/pex-1.2.15.tar.gz"
PY_WHEEL = "https://pypi.python.org/packages/53/67/9620edf7803ab867b175e4fd23c7b8bd8eba11cb761514dcd2e726ef07da/py-1.4.34-py2.py3-none-any.whl"
PYTEST_WHEEL = "https://pypi.python.org/packages/fd/3e/d326a05d083481746a769fc051ae8d25f574ef140ad4fe7f809a2b63c0f0/pytest-3.1.3-py2.py3-none-any.whl"
REQUESTS_SRC = "https://pypi.python.org/packages/d9/03/155b3e67fe35fe5b6f4227a8d9e96a14fda828b18199800d161bcefc1359/requests-2.12.3.tar.gz"
SETUPTOOLS_SRC = "https://pypi.python.org/packages/68/13/1bfbfbd86560e61fa9803d241084fff41a775bf56ee8b3ad72fc9e550dad/setuptools-31.0.0.tar.gz"
VIRTUALENV_SRC = "https://pypi.python.org/packages/d4/0c/9840c08189e030873387a73b90ada981885010dd9aea134d6de30cd24cb8/virtualenv-15.1.0.tar.gz"
VIRTUALENV_PREFIX = "virtualenv-15.1.0"
WHEEL_SRC = "https://pypi.python.org/packages/c9/1d/bd19e691fd4cfe908c76c429fe6e4436c9e83583c4414b54f6c85471954a/wheel-0.29.0.tar.gz"

http_file(
    name = "pytest_whl",
    downloaded_file_path = "pytest-3.1.3-py2.py3-none-any.whl",
    urls = [PYTEST_WHEEL],
)

http_file(
    name = "py_whl",
    downloaded_file_path = "py-1.4.34-py2.py3-none-any.whl",
    urls = [PY_WHEEL],
)

http_file(
    name = "wheel_src",
    downloaded_file_path = "wheel-0.29.0.tar.gz",
    urls = [WHEEL_SRC],
)

http_file(
    name = "pex_src",
    downloaded_file_path = "pex-1.2.15.tar.gz",
    urls = [PEX_SRC],
)

http_file(
    name = "requests_src",
    downloaded_file_path = "requests-2.12.3.tar.gz",
    urls = [REQUESTS_SRC],
)

http_file(
    name = "setuptools_src",
    downloaded_file_path = "setuptools-31.0.0.tar.gz",
    urls = [SETUPTOOLS_SRC],
)

http_archive(
    name = "virtualenv",
    urls = [VIRTUALENV_SRC],
    strip_prefix = VIRTUALENV_PREFIX,
    build_file_content = "\n".join([
        "py_binary(",
        "    name = 'virtualenv',",
        "    srcs = ['virtualenv.py'],",
        "    data = glob(['**/*']),",
        "    visibility = ['//visibility:public'],",
        ")",
    ]),
    sha256 = "02f8102c2436bb03b3ee6dede1919d1dac8a427541652e5ec95171ec8adbc93a",
)
# end pex repos

# protobuf dependencies for C++ and Java
http_archive(
    name = "com_google_protobuf",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.6.1.3.tar.gz"],
    strip_prefix = "protobuf-3.6.1.3",
    sha256 = "73fdad358857e120fd0fa19e071a96e15c0f23bb25f85d3f7009abfd4f264a2a",
)
# end protobuf dependencies for C++ and Java

# 3rdparty C++ dependencies
http_archive(
    name = "com_github_gflags_gflags",
    urls = ["https://github.com/gflags/gflags/archive/v2.2.1.tar.gz"],
    strip_prefix = "gflags-2.2.1",
    sha256 = "ae27cdbcd6a2f935baa78e4f21f675649271634c092b1be01469440495609d0e",
)

http_archive(
    name = "org_libevent_libevent",
    urls = ["https://github.com/libevent/libevent/releases/download/release-2.1.10-stable/libevent-2.1.10-stable.tar.gz"],
    strip_prefix = "libevent-2.1.10-stable",
    build_file = "@//:third_party/libevent/libevent.BUILD",
    sha256 = "e864af41a336bb11dab1a23f32993afe963c1f69618bd9292b89ecf6904845b0",
)

http_archive(
    name = "org_nongnu_libunwind",
    urls = ["https://download.savannah.nongnu.org/releases/libunwind/libunwind-1.1.tar.gz"],
    strip_prefix = "libunwind-1.1",
    build_file = "@//:third_party/libunwind/libunwind.BUILD",
    sha256 = "9dfe0fcae2a866de9d3942c66995e4b460230446887dbdab302d41a8aee8d09a",
)

http_archive(
    name = "org_apache_zookeeper",
    urls = ["https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz"],
    strip_prefix = "zookeeper-3.4.14",
    build_file = "@//:third_party/zookeeper/zookeeper.BUILD",
    sha256 = "b14f7a0fece8bd34c7fffa46039e563ac5367607c612517aa7bd37306afbd1cd",
)

http_archive(
    name = "com_github_gperftools_gperftools",
    urls = ["https://github.com/gperftools/gperftools/releases/download/gperftools-2.4/gperftools-2.4.tar.gz"],
    strip_prefix = "gperftools-2.4",
    build_file = "@//:third_party/gperftools/gperftools.BUILD",
    sha256 = "982a37226eb42f40714e26b8076815d5ea677a422fb52ff8bfca3704d9c30a2d",
)

http_archive(
    name = "com_github_google_glog",
    urls = ["https://github.com/google/glog/archive/v0.3.5.tar.gz"],
    strip_prefix = "glog-0.3.5",
    build_file = "@//:third_party/glog/glog.BUILD",
    sha256 = "7580e408a2c0b5a89ca214739978ce6ff480b5e7d8d7698a2aa92fadc484d1e0",
)

http_archive(
    name = "com_google_googletest",
    urls = ["https://github.com/google/googletest/archive/release-1.8.0.tar.gz"],
    strip_prefix = "googletest-release-1.8.0",
    build_file = "@//:third_party/gtest/gtest.BUILD",
    sha256 = "58a6f4277ca2bc8565222b3bbd58a177609e9c488e8a72649359ba51450db7d8",
)

http_archive(
    name = "com_github_cereal",
    urls = ["https://github.com/USCiLab/cereal/archive/v1.2.2.tar.gz"],
    strip_prefix = "cereal-1.2.2",
    build_file = "@//:third_party/cereal/cereal.BUILD",
    sha256 = "1921f26d2e1daf9132da3c432e2fd02093ecaedf846e65d7679ddf868c7289c4",
)

http_archive(
    name = "com_github_jbeder_yaml_cpp",
    urls = ["https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.6.2.tar.gz"],
    strip_prefix = "yaml-cpp-yaml-cpp-0.6.2",
    build_file = "@//:third_party/yaml-cpp/yaml.BUILD",
    sha256 = "e4d8560e163c3d875fd5d9e5542b5fd5bec810febdcba61481fe5fc4e6b1fd05",
)

http_archive(
    name = "com_github_danmar_cppcheck",
    build_file = "@//:third_party/cppcheck/cppcheck.BUILD",
    patch_args = ["-p2"],
    patches = ["//third_party/cppcheck:cppcheck-readdir-fix.patch"],
    sha256 = "cb0e66cbe2d6b655fce430cfaaa74b83ad11c91f221e3926f1ca3211bb7c906b",
    strip_prefix = "cppcheck-1.90",
    urls = ["https://github.com/danmar/cppcheck/archive/1.90.zip"],
)

http_archive(
    name = "com_github_hopscotch_hashmap",
    build_file = "@//:third_party/hopscotch-hashmap/hopscotch.BUILD",
    sha256 = "73e301925e1418c5ed930ef37ebdcab2c395a6d1bdaf5a012034bb75307d33f1",
    strip_prefix = "hopscotch-map-2.2.1",
    urls = ["https://github.com/Tessil/hopscotch-map/archive/v2.2.1.tar.gz"],
)
# end 3rdparty C++ dependencies

# for helm
http_archive(
    name = "helm_mac",
    urls = ["https://get.helm.sh/helm-v3.0.2-darwin-amd64.tar.gz"],
    strip_prefix = "darwin-amd64",
    build_file = "@//:third_party/helm/helm.BUILD",
    sha256 = "05c7748da0ea8d5f85576491cd3c615f94063f20986fd82a0f5658ddc286cdb1",
)

http_archive(
    name = "helm_linux",
    urls = ["https://get.helm.sh/helm-v3.0.2-linux-amd64.tar.gz"],
    strip_prefix = "linux-amd64",
    build_file = "@//:third_party/helm/helm.BUILD",
    sha256 = "c6b7aa7e4ffc66e8abb4be328f71d48c643cb8f398d95c74d075cfb348710e1d",
)
# end helm

# for docker image building
http_archive(
    name = "io_bazel_rules_docker",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.7.0.tar.gz"],
    strip_prefix = "rules_docker-0.7.0",
    sha256 = "aed1c249d4ec8f703edddf35cbe9dfaca0b5f5ea6e4cd9e83e99f3b0d1136c3d",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

container_pull(
    name = "heron-base",
    registry = "index.docker.io",
    repository = "heron/base",
    tag = "0.4.0",
    digest = "sha256:495800e9eb001dfd2fb41d1941155203bb9be06b716b0f8b1b0133eb12ea813c"
)

# end docker image building

# for nomad repear
http_archive(
    name = "nomad_mac",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_darwin_amd64.zip"],
    build_file = "@//:third_party/nomad/nomad.BUILD",
    sha256 = "53452f5bb27131f1fe5a5f9178324511bcbc54e4fef5bec4e25b049ac38e0632",
)

http_archive(
    name = "nomad_linux",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_linux_amd64.zip"],
    build_file = "@//:third_party/nomad/nomad.BUILD",
    sha256 = "b3b78dccbdbd54ddc7a5ffdad29bce2d745cac93ea9e45f94e078f57b756f511",
)

# scala integration
rules_scala_version = "358ab829626c6c2d34ec27f856485d3121e299c7"  # Jan 15 2020 - update this as needed

http_archive(
    name = "io_bazel_rules_scala",
    strip_prefix = "rules_scala-%s" % rules_scala_version,
    type = "zip",
    url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
)

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

scala_repositories(("2.12.8", {
    "scala_compiler": "f34e9119f45abd41e85b9e121ba19dd9288b3b4af7f7047e86dc70236708d170",
    "scala_library": "321fb55685635c931eba4bc0d7668349da3f2c09aee2de93a70566066ff25c28",
    "scala_reflect": "4d6405395c4599ce04cea08ba082339e3e42135de9aae2923c9f5367e957315a"
}))

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

scala_register_toolchains()
