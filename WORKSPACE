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

RULES_JVM_EXTERNAL_TAG = "3.1"
RULES_JVM_EXTERNAL_SHA = "e246373de2353f3d34d35814947aa8b7d0dd1a58c2f7a6c41cfeaff3007c2d14"

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

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
jersey_version = "2.25.1"
hk2_api = "2.5.0-b32"
kubernetes_client_version = "7.0.0"
squareup_okhttp_version = "3.14.5"

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

load("@rules_jvm_external//migration:maven_jar_migrator_deps.bzl", "maven_jar_migrator_repositories")
maven_jar_migrator_repositories()

maven_install(
    name = "maven",
    artifacts = [
    "antlr:antlr:2.7.7",
    "org.apache.zookeeper:zookeeper:3.4.14",
    "io.kubernetes:client-java:" + kubernetes_client_version,
    "com.esotericsoftware:kryo:3.0.3",
    "org.apache.avro:avro:1.7.4",
    "org.apache.mesos:mesos:0.22.0",
    "com.hashicorp.nomad:nomad-sdk:0.7.0",
    "org.apache.hadoop:hadoop-core:0.20.2",
    "org.apache.pulsar:pulsar-client:shaded:1.19.0-incubating",
    "com.google.apis:google-api-services-storage:v1-rev108-" + google_client_version,
    "org.apache.reef:reef-runtime-yarn:" + reef_version,
    "org.apache.reef:reef-runtime-local:" + reef_version,
    "org.apache.httpcomponents:httpclient:" + http_client_version,
    "org.apache.httpcomponents:httpmime:" + http_client_version,
    "com.google.apis:google-api-services-storage:v1-rev108-1.22.0",
    "org.apache.pulsar:pulsar-client:jar:shaded:1.19.0-incubating",
    "io.kubernetes:client-java:7.0.0",
    "com.hashicorp.nomad:nomad-sdk:0.7.0",
    "com.microsoft.dhalion:dhalion:0.2.3",
    "org.objenesis:objenesis:2.1",
    "org.ow2.asm:asm-all:5.1",
    "org.ow2.asm:asm:5.0.4",
    "com.amazonaws:aws-java-sdk-s3:" + aws_version,
    "org.eclipse.jetty:jetty-server:" + jetty_version,
    "org.eclipse.jetty:jetty-http:" + jetty_version,
    "org.eclipse.jetty:jetty-security:" + jetty_version,
    "org.eclipse.jetty:jetty-continuation:" + jetty_version,
    "org.eclipse.jetty:jetty-servlets:" + jetty_version,
    "org.eclipse.jetty:jetty-servlet:" + jetty_version,
    "org.jvnet.mimepull:mimepull:1.9.7",
    "javax.servlet:javax.servlet-api:3.1.0",
    "org.glassfish.jersey.media:jersey-media-json-jackson:" + jersey_version,
    "org.glassfish.jersey.media:jersey-media-multipart:" + jersey_version,
    "org.glassfish.jersey.containers:jersey-container-servlet:" + jersey_version,
    "org.apache.distributedlog:distributedlog-core-shaded:" + distributedlog_version,
    "io.netty:netty-all:4.1.22.Final",
    "aopalliance:aopalliance:1.0",
    "org.roaringbitmap:RoaringBitmap:0.6.51",
    "com.google.guava:guava:18.0",
    "io.gsonfire:gson-fire:1.8.3",
    "org.apache.curator:curator-framework:" + curator_version,
    "org.apache.curator:curator-recipes:" + curator_version,
    "org.apache.curator:curator-client:" + curator_version,
    "org.slf4j:slf4j-api:" + slf4j_version,
    "org.slf4j:slf4j-jdk14:" + slf4j_version,
    "log4j:log4j:1.2.17",
    "org.yaml:snakeyaml:1.15",
    "tech.tablesaw:tablesaw-core:0.11.4",
    "org.glassfish.hk2.external:aopalliance-repackaged:2.5.0-b32",
    "org.apache.commons:commons-compress:1.14",
    "commons-io:commons-io:2.4",
    "commons-collections:commons-collections:3.2.1",
    "commons-cli:commons-cli:1.3.1",
    "org.apache.commons:commons-compress:1.14",
    "com.jayway.jsonpath:json-path:2.1.0",
    "com.fasterxml.jackson.core:jackson-core:" + jackson_version,
    "com.fasterxml.jackson.core:jackson-annotations:" + jackson_version,
    "com.fasterxml.jackson.core:jackson-databind:" + jackson_version,
    "com.fasterxml.jackson.jaxrs:jackson-jaxrs-base:2.8.8",
    "com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:2.8.8",
    "org.mockito:mockito-all:1.10.19",
    "org.sonatype.plugins:jarjar-maven-plugin:1.9",
    "org.powermock:powermock-api-mockito:" + powermock_version,
    "org.powermock:powermock-module-junit4:" + powermock_version,
    "com.puppycrawl.tools:checkstyle:6.17",
    "com.googlecode.json-simple:json-simple:1.1",
    maven.artifact(
      group = "org.apache.httpcomponents",
      artifact = "httpclient",
      version = http_client_version,
      classifier = "tests",
      packaging = "test-jar",
    )
    ],
    repositories = [
        "https://jcenter.bintray.com",
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
    fetch_sources = True,
    version_conflict_policy = "pinned",
    # See https://github.com/bazelbuild/rules_jvm_external/#repository-aliases
    # This can be removed if none of your external dependencies uses `maven_jar`.
    # generate_compat_repositories = True,
)
# load("@maven//:compat.bzl", "compat_repositories")
# compat_repositories()

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
    urls = ["https://github.com/danmar/cppcheck/archive/1.87.zip"],
    strip_prefix = "cppcheck-1.87",
    build_file = "@//:third_party/cppcheck/cppcheck.BUILD",
    sha256 = "b3de7fbdc1a23d7341b55f7f88877e106a76847bd5a07fa721c07310b625318b",
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
