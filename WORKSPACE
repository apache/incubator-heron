# versions shared across artifacts that should be upgraded together
aws_version = "1.10.66"
curator_version = "2.9.0"
httpcomponents_version = "4.3"
jackson_version = "2.6.3"
powermock_version = "1.6.2"
slf4j_version = "1.7.7"

maven_server(
  name = "default",
  url = "http://central.maven.org/maven2/",
)

maven_server(
  name = "twitter-maven",
  url = "http://maven.twttr.com",
)

maven_jar(
  name = "antlr",
  artifact = "antlr:antlr:2.7.7",
)

maven_jar(
  name = "asm-all",
  artifact = "org.ow2.asm:asm-all:5.1",
)

maven_jar(
  name = "aws-java-sdk-core",
  artifact = "com.amazonaws:aws-java-sdk-core:" + aws_version,
)

maven_jar(
  name = "aws-java-sdk-s3",
  artifact = "com.amazonaws:aws-java-sdk-s3:" + aws_version,
)

maven_jar(
  name = "checkstyle",
  artifact = "com.puppycrawl.tools:checkstyle:6.17",
)

maven_jar(
  name = "commons-beanutils",
  artifact = "commons-beanutils:commons-beanutils:1.9.2",
)

maven_jar(
  name = "commons-io",
  artifact = "commons-io:commons-io:2.4",
)

maven_jar(
  name = "commons-cli",
  artifact = "commons-cli:commons-cli:1.3.1",
)

maven_jar(
  name = "commons-collections",
  artifact = "commons-collections:commons-collections:3.2.1",
)

maven_jar(
  name = "commons-lang",
  artifact = "commons-lang:commons-lang:2.6",
)

maven_jar(
  name = "commons-logging",
  artifact = "commons-logging:commons-logging:1.1.1",
)

maven_jar(
  name = "curator-framework",
  artifact = "org.apache.curator:curator-framework:" + curator_version,
)

maven_jar(
  name = "curator-client",
  artifact = "org.apache.curator:curator-client:" + curator_version,
)

maven_jar(
  name = "guava",
  artifact = "com.google.guava:guava:18.0",
)

maven_jar(
  name = "hadoop-core",
  artifact = "org.apache.hadoop:hadoop-core:0.20.2",
)

maven_jar(
  name = "http-client",
  artifact = "org.apache.httpcomponents:httpclient:" + httpcomponents_version,
)

maven_jar(
  name = "http-core",
  artifact = "org.apache.httpcomponents:httpcore:" + httpcomponents_version,
)

maven_jar(
  name = "jackson-annotations",
  artifact = "com.fasterxml.jackson.core:jackson-annotations:" + jackson_version,
)

maven_jar(
  name = "jackson-core",
  artifact = "com.fasterxml.jackson.core:jackson-core:" + jackson_version,
)

maven_jar(
  name = "jackson-databind",
  artifact = "com.fasterxml.jackson.core:jackson-databind:" + jackson_version,
)

maven_jar(
  name = "javassist",
  artifact = "org.javassist:javassist:3.18.1-GA",
)

maven_jar(
  name = "jarjar",
  artifact = "org.sonatype.plugins:jarjar-maven-plugin:1.9",
)

maven_jar(
  name = "json-simple",
  artifact = "com.googlecode.json-simple:json-simple:1.1",
)

maven_jar(
  name = "kryo",
  artifact = "com.esotericsoftware:kryo:3.0.3",
)

maven_jar(
  name = "log4j-over-slf4j",
  artifact = "org.slf4j:log4j-over-slf4j:" + slf4j_version
)

maven_jar(
  name = "mesos",
  artifact = "org.apache.mesos:mesos:0.22.0",
)

maven_jar(
  name = "minlog",
  artifact = "com.esotericsoftware:minlog:1.3.0",
)

maven_jar(
  name = "objenesis",
  artifact = "org.objenesis:objenesis:2.1",
)

maven_jar(
  name = "powermock-api-mockito",
  artifact = "org.powermock:powermock-api-mockito:" + powermock_version,
)

maven_jar(
  name = "powermock-api-support",
  artifact = "org.powermock:powermock-api-support:" + powermock_version,
)

maven_jar(
  name = "powermock-core",
  artifact = "org.powermock:powermock-core:" + powermock_version,
)

maven_jar(
  name = "powermock-module-junit4",
  artifact = "org.powermock:powermock-module-junit4:" + powermock_version,
)

maven_jar(
  name = "powermock-module-junit4-common",
  artifact = "org.powermock:powermock-module-junit4-common:" + powermock_version,
)

maven_jar(
  name = "powermock-reflect",
  artifact = "org.powermock:powermock-reflect:" + powermock_version,
)

maven_jar(
  name = "protobuf-java",
  artifact = "com.google.protobuf:protobuf-java:2.5.0",
)

maven_jar(
  name = "slf4j-api",
  artifact = "org.slf4j:slf4j-api:" + slf4j_version
)

maven_jar(
  name = "slf4j-jdk",
  artifact = "org.slf4j:slf4j-jdk14:" + slf4j_version
)

maven_jar(
  name = "snakeyaml",
  artifact = "org.yaml:snakeyaml:1.15",
)

maven_jar(
  name = "thrift",
  artifact = "org.apache.thrift:libthrift:0.5.0-1",
  server = "twitter-maven",
)

maven_jar(
  name = "zookeeper",
  artifact = "org.apache.zookeeper:zookeeper:3.4.6",
)
