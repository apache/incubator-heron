# versions shared across artifacts that should be upgraded together
aws_version = "1.11.58"
curator_version = "2.9.0"
jackson_version = "2.6.6"
powermock_version = "1.6.2"
reef_version = "0.14.0"
slf4j_version = "1.7.7"

maven_server(
  name = "default",
  url = "http://central.maven.org/maven2/",
)

maven_jar(
  name = "org_apache_avro_avro",
  artifact = "org.apache.avro:avro:1.7.4"
)

maven_server(
  name = "maven_twttr_com",
  url = "http://maven.twttr.com",
)

maven_jar(
  name = "antlr_antlr",
  artifact = "antlr:antlr:2.7.7",
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
  name = "org_apache_hadoop_hadoop_core",
  artifact = "org.apache.hadoop:hadoop-core:0.20.2",
)

maven_jar(
  name = "org_apache_httpcomponents_http_client",
  artifact = "org.apache.httpcomponents:httpclient:4.5.2",
)

maven_jar(
  name = "org_apache_httpcomponents_http_core",
  artifact = "org.apache.httpcomponents:httpcore:4.4.5",
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
  name = "org_apache_mesos_mesos",
  artifact = "org.apache.mesos:mesos:0.22.0",
)

maven_jar(
  name = "com_esotericsoftware_minlog",
  artifact = "com.esotericsoftware:minlog:1.3.0",
)

maven_jar(
  name = "io_netty_netty_all",
  artifact = "io.netty:netty-all:4.0.21.Final"
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
  name = "com_google_protobuf_protobuf_java",
  artifact = "com.google.protobuf:protobuf-java:2.5.0",
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
  name = "org_apache_reef_tang",
  artifact = "org.apache.reef:tang:" + reef_version
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
  artifact = "org.apache.zookeeper:zookeeper:3.4.6",
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
  artifact = "org.apache.kafka:kafka-clients:0.8.2.1",
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
