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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

RULES_JVM_EXTERNAL_TAG = "3.1"

RULES_JVM_EXTERNAL_SHA = "e246373de2353f3d34d35814947aa8b7d0dd1a58c2f7a6c41cfeaff3007c2d14"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

# versions shared across artifacts that should be upgraded together
aws_version = "1.11.58"

curator_version = "2.9.0"

google_client_version = "1.22.0"

jackson_version = "2.8.8"

powermock_version = "1.6.2"

reef_version = "0.14.0"

slf4j_version = "1.7.30"

distributedlog_version = "4.11.0"

http_client_version = "4.5.2"

# heron API server
jetty_version = "9.4.6.v20170531"

jersey_version = "2.25.1"

kubernetes_client_version = "8.0.0"

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")
load("@rules_jvm_external//migration:maven_jar_migrator_deps.bzl", "maven_jar_migrator_repositories")

maven_jar_migrator_repositories()

maven_install(
    name = "maven",
    artifacts = [
        "antlr:antlr:2.7.7",
        "org.apache.zookeeper:zookeeper:3.5.8",
        "io.kubernetes:client-java:" + kubernetes_client_version,
        "com.esotericsoftware:kryo:5.0.0",
        "org.apache.avro:avro:1.7.4",
        "org.apache.mesos:mesos:0.22.0",
        "com.hashicorp.nomad:nomad-sdk:0.7.0",
        "org.apache.hadoop:hadoop-core:0.20.2",
        "org.apache.pulsar:pulsar-client:jar:shaded:1.19.0-incubating",
        "org.apache.kafka:kafka-clients:2.2.0",
        "com.google.apis:google-api-services-storage:v1-rev108-" + google_client_version,
        "org.apache.reef:reef-runtime-yarn:" + reef_version,
        "org.apache.reef:reef-runtime-local:" + reef_version,
        "org.apache.httpcomponents:httpclient:" + http_client_version,
        "org.apache.httpcomponents:httpmime:" + http_client_version,
        "com.google.apis:google-api-services-storage:v1-rev108-1.22.0",
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
        "javax.xml.bind:jaxb-api:2.3.0",
        "javax.activation:activation:1.1.1",
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
        ),
    ],
    fetch_sources = True,
    maven_install_json = "//:maven_install.json",
    repositories = [
        "https://jcenter.bintray.com",
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
    version_conflict_policy = "pinned",
)

# https://github.com/bazelbuild/rules_jvm_external#updating-maven_installjson
# To update `maven_install.json` run the following command:
# `bazel run @unpinned_maven//:pin`
load("@maven//:defs.bzl", "pinned_maven_install")

pinned_maven_install()

http_archive(
    name = "rules_python",
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.1.0/rules_python-0.1.0.tar.gz",
)

load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()
# Only needed if using the packaging rules.
# load("@rules_python//python:pip.bzl", "pip_repositories")
# pip_repositories()

# for pex repos
PEX_WHEEL = "https://pypi.python.org/packages/18/92/99270775cfc5ddb60c19588de1c475f9ff2837a6e0bbd5eaa5286a6a472b/pex-2.1.9-py2.py3-none-any.whl"

PY_WHEEL = "https://pypi.python.org/packages/53/67/9620edf7803ab867b175e4fd23c7b8bd8eba11cb761514dcd2e726ef07da/py-1.4.34-py2.py3-none-any.whl"

PYTEST_WHEEL = "https://pypi.python.org/packages/fd/3e/d326a05d083481746a769fc051ae8d25f574ef140ad4fe7f809a2b63c0f0/pytest-3.1.3-py2.py3-none-any.whl"

REQUESTS_SRC = "https://pypi.python.org/packages/d9/03/155b3e67fe35fe5b6f4227a8d9e96a14fda828b18199800d161bcefc1359/requests-2.12.3.tar.gz"

SETUPTOOLS_WHEEL = "https://pypi.python.org/packages/a0/df/635cdb901ee4a8a42ec68e480c49f85f4c59e8816effbf57d9e6ee8b3588/setuptools-46.1.3-py3-none-any.whl"

WHEEL_SRC = "https://pypi.python.org/packages/c9/1d/bd19e691fd4cfe908c76c429fe6e4436c9e83583c4414b54f6c85471954a/wheel-0.29.0.tar.gz"

http_file(
    name = "pytest_whl",
    downloaded_file_path = "pytest-3.1.3-py2.py3-none-any.whl",
    sha256 = "2a4f483468954621fcc8f74784f3b42531e5b5008d49fc609b37bc4dbc6dead1",
    urls = [PYTEST_WHEEL],
)

http_file(
    name = "py_whl",
    downloaded_file_path = "py-1.4.34-py2.py3-none-any.whl",
    sha256 = "2ccb79b01769d99115aa600d7eed99f524bf752bba8f041dc1c184853514655a",
    urls = [PY_WHEEL],
)

http_file(
    name = "wheel_src",
    downloaded_file_path = "wheel-0.29.0.tar.gz",
    sha256 = "1ebb8ad7e26b448e9caa4773d2357849bf80ff9e313964bcaf79cbf0201a1648",
    urls = [WHEEL_SRC],
)

http_file(
    name = "pex_src",
    downloaded_file_path = "pex-2.1.9-py2.py3-none-any.whl",
    sha256 = "5cad8d960c187541f71682fc938a843ef9092aab46f27b33ace7e570325e2626",
    urls = [PEX_WHEEL],
)

http_file(
    name = "requests_src",
    downloaded_file_path = "requests-2.12.3.tar.gz",
    sha256 = "de5d266953875e9647e37ef7bfe6ef1a46ff8ddfe61b5b3652edf7ea717ee2b2",
    urls = [REQUESTS_SRC],
)

http_file(
    name = "setuptools_wheel",
    downloaded_file_path = "setuptools-46.1.3-py3-none-any.whl",
    sha256 = "4fe404eec2738c20ab5841fa2d791902d2a645f32318a7850ef26f8d7215a8ee",
    urls = [SETUPTOOLS_WHEEL],
)

# end pex repos

# protobuf dependencies for C++ and Java
http_archive(
    name = "com_google_protobuf",
    sha256 = "03d2e5ef101aee4c2f6ddcf145d2a04926b9c19e7086944df3842b1b8502b783",
    strip_prefix = "protobuf-3.8.0",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.8.0.tar.gz"],
)
# end protobuf dependencies for C++ and Java

# 3rdparty C++ dependencies
http_archive(
    name = "com_github_gflags_gflags",
    sha256 = "ae27cdbcd6a2f935baa78e4f21f675649271634c092b1be01469440495609d0e",
    strip_prefix = "gflags-2.2.1",
    urls = ["https://github.com/gflags/gflags/archive/v2.2.1.tar.gz"],
)

http_archive(
    name = "org_libevent_libevent",
    build_file = "@//:third_party/libevent/libevent.BUILD",
    sha256 = "e864af41a336bb11dab1a23f32993afe963c1f69618bd9292b89ecf6904845b0",
    strip_prefix = "libevent-2.1.10-stable",
    urls = ["https://github.com/libevent/libevent/releases/download/release-2.1.10-stable/libevent-2.1.10-stable.tar.gz"],
)

http_archive(
    name = "org_nongnu_libunwind",
    build_file = "@//:third_party/libunwind/libunwind.BUILD",
    sha256 = "0a4b5a78d8c0418dfa610245f75fa03ad45d8e5e4cc091915d2dbed34c01178e",
    strip_prefix = "libunwind-1.3.2",
    urls = ["https://github.com/libunwind/libunwind/releases/download/v1.3.2/libunwind-1.3.2.tar.gz"],
)

http_archive(
    name = "org_apache_zookeeper",
    build_file = "@//:third_party/zookeeper/BUILD",
    strip_prefix = "apache-zookeeper-3.5.8",
    urls = ["https://archive.apache.org/dist/zookeeper/zookeeper-3.5.8/apache-zookeeper-3.5.8.tar.gz"],
)

http_archive(
    name = "com_github_gperftools_gperftools",
    build_file = "@//:third_party/gperftools/gperftools.BUILD",
    sha256 = "982a37226eb42f40714e26b8076815d5ea677a422fb52ff8bfca3704d9c30a2d",
    strip_prefix = "gperftools-2.4",
    urls = ["https://github.com/gperftools/gperftools/releases/download/gperftools-2.4/gperftools-2.4.tar.gz"],
)

http_archive(
    name = "com_github_google_glog",
    build_file = "@//:third_party/glog/glog.BUILD",
    sha256 = "7580e408a2c0b5a89ca214739978ce6ff480b5e7d8d7698a2aa92fadc484d1e0",
    strip_prefix = "glog-0.3.5",
    urls = ["https://github.com/google/glog/archive/v0.3.5.tar.gz"],
)

http_archive(
    name = "com_google_googletest",
    build_file = "@//:third_party/gtest/gtest.BUILD",
    sha256 = "58a6f4277ca2bc8565222b3bbd58a177609e9c488e8a72649359ba51450db7d8",
    strip_prefix = "googletest-release-1.8.0",
    urls = ["https://github.com/google/googletest/archive/release-1.8.0.tar.gz"],
)

http_archive(
    name = "com_github_cereal",
    build_file = "@//:third_party/cereal/cereal.BUILD",
    sha256 = "1921f26d2e1daf9132da3c432e2fd02093ecaedf846e65d7679ddf868c7289c4",
    strip_prefix = "cereal-1.2.2",
    urls = ["https://github.com/USCiLab/cereal/archive/v1.2.2.tar.gz"],
)

http_archive(
    name = "com_github_jbeder_yaml_cpp",
    build_file = "@//:third_party/yaml-cpp/yaml.BUILD",
    sha256 = "e4d8560e163c3d875fd5d9e5542b5fd5bec810febdcba61481fe5fc4e6b1fd05",
    strip_prefix = "yaml-cpp-yaml-cpp-0.6.2",
    urls = ["https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.6.2.tar.gz"],
)

http_archive(
    name = "com_github_corvusoft_kashmir_cpp",
    build_file = "@//:third_party/kashmir/kashmir.BUILD",
    patch_args = ["-p1"],
    patches = ["//third_party/kashmir:kashmir-random-fix.patch"],
    sha256 = "c3515d6c7a470663f06b79bb23cbb2ff2f3feab4c2a333f783edc0a802f1d062",
    strip_prefix = "kashmir-dependency-19fb1d5c14866bd5202c2458baf50263001a9cb0",
    urls = ["https://github.com/Corvusoft/kashmir-dependency/archive/19fb1d5c14866bd5202c2458baf50263001a9cb0.zip"],
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
    build_file = "@//:third_party/helm/helm.BUILD",
    sha256 = "05c7748da0ea8d5f85576491cd3c615f94063f20986fd82a0f5658ddc286cdb1",
    strip_prefix = "darwin-amd64",
    urls = ["https://get.helm.sh/helm-v3.0.2-darwin-amd64.tar.gz"],
)

http_archive(
    name = "helm_linux",
    build_file = "@//:third_party/helm/helm.BUILD",
    sha256 = "c6b7aa7e4ffc66e8abb4be328f71d48c643cb8f398d95c74d075cfb348710e1d",
    strip_prefix = "linux-amd64",
    urls = ["https://get.helm.sh/helm-v3.0.2-linux-amd64.tar.gz"],
)
# end helm

# for docker image building
DOCKER_RULES_VERSION = "0.15.0"

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "1698624e878b0607052ae6131aa216d45ebb63871ec497f26c67455b34119c80",
    strip_prefix = "rules_docker-%s" % DOCKER_RULES_VERSION,
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v%s.tar.gz" % DOCKER_RULES_VERSION],
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)
container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//repositories:py_repositories.bzl", "py_deps")

py_deps()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

container_pull(
    name = "heron-base",
    digest = "sha256:495800e9eb001dfd2fb41d1941155203bb9be06b716b0f8b1b0133eb12ea813c",
    registry = "index.docker.io",
    repository = "heron/base",
    tag = "0.5.0",
)
# end docker image building

http_archive(
    name = "rules_pkg",
    urls = [
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.2.6/rules_pkg-0.2.6.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.2.6/rules_pkg-0.2.6.tar.gz",
    ],
    sha256 = "aeca78988341a2ee1ba097641056d168320ecc51372ef7ff8e64b139516a4937",
)
load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
rules_pkg_dependencies()

# scala integration
rules_scala_version = "358ab829626c6c2d34ec27f856485d3121e299c7"  # Jan 15 2020 - update this as needed

http_archive(
    name = "io_bazel_rules_scala",
    strip_prefix = "rules_scala-%s" % rules_scala_version,
    sha256 = "5abd638278de10ccccb0b4d614158f394278b828708ba990461334ecc01529a6",
    type = "zip",
    url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
)

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

scala_repositories((
    "2.12.8",
    {
        "scala_compiler": "f34e9119f45abd41e85b9e121ba19dd9288b3b4af7f7047e86dc70236708d170",
        "scala_library": "321fb55685635c931eba4bc0d7668349da3f2c09aee2de93a70566066ff25c28",
        "scala_reflect": "4d6405395c4599ce04cea08ba082339e3e42135de9aae2923c9f5367e957315a",
    },
))

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

scala_register_toolchains()
