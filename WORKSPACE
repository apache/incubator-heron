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

RULES_JVM_EXTERNAL_TAG = "4.2"
RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:defs.bzl", "artifact")
load("@rules_jvm_external//:specs.bzl", "maven")

# versions shared across artifacts that should be upgraded together
aws_version = "1.11.58"

curator_version = "5.1.0"

google_client_version = "1.22.0"

jackson_version = "2.8.8"

powermock_version = "1.6.2"

reef_version = "0.14.0"

slf4j_version = "1.7.30"

distributedlog_version = "4.13.0"

http_client_version = "4.5.2"

# heron API server
jetty_version = "9.4.6.v20170531"

jersey_version = "2.25.1"

kubernetes_client_version = "14.0.0"

maven_install(
    name = "maven",
    artifacts = [
        "antlr:antlr:2.7.7",
        "org.apache.zookeeper:zookeeper:3.6.3",
        "io.kubernetes:client-java:" + kubernetes_client_version,
        "io.kubernetes:client-java-api-fluent:" + kubernetes_client_version,
        "com.esotericsoftware:kryo:5.2.0",
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
        "com.microsoft.dhalion:dhalion:0.2.6",
        "org.objenesis:objenesis:2.1",
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
        "org.apache.distributedlog:distributedlog-core:" + distributedlog_version,
        "io.netty:netty-all:4.1.72.Final",
        "aopalliance:aopalliance:1.0",
        "org.roaringbitmap:RoaringBitmap:0.6.51",
        "com.google.guava:guava:23.6-jre",
        "io.gsonfire:gson-fire:1.8.3",
        "org.apache.curator:curator-framework:" + curator_version,
        "org.apache.curator:curator-recipes:" + curator_version,
        "org.apache.curator:curator-client:" + curator_version,
        "org.slf4j:slf4j-api:" + slf4j_version,
        "org.slf4j:slf4j-jdk14:" + slf4j_version,
        "log4j:log4j:1.2.17",
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
    fail_if_repin_required = True,
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
# `REPIN=1 bazel run @unpinned_maven//:pin`
load("@maven//:defs.bzl", "pinned_maven_install")
pinned_maven_install()

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_github_johnynek_bazel_jar_jar",
    commit = "171f268569384c57c19474b04aebe574d85fde0d", # Latest commit SHA as at 2019/02/13
    remote = "git://github.com/johnynek/bazel_jar_jar.git",
    shallow_since = "1594234634 -1000",
)

load(
    "@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl",
    "jar_jar_repositories",
)
jar_jar_repositories()

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_archive(
    name = "platforms",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.5/platforms-0.0.5.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/0.0.5/platforms-0.0.5.tar.gz",
    ],
    sha256 = "379113459b0feaf6bfbb584a91874c065078aa673222846ac765f86661c27407",
)

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
PEX_PKG = "https://files.pythonhosted.org/packages/d4/73/4c76e06824baadba81b39125721c97fb22e201b35fcd17b32b5a5fa77c59/pex-2.1.62-py2.py3-none-any.whl"

PYTEST_PKG = "https://files.pythonhosted.org/packages/40/76/86f886e750b81a4357b6ed606b2bcf0ce6d6c27ad3c09ebf63ed674fc86e/pytest-6.2.5-py3-none-any.whl"

REQUESTS_PKG = "https://files.pythonhosted.org/packages/2d/61/08076519c80041bc0ffa1a8af0cbd3bf3e2b62af10435d269a9d0f40564d/requests-2.27.1-py2.py3-none-any.whl"

SETUPTOOLS_PKG = "https://files.pythonhosted.org/packages/3d/f2/1489d3b6c72d68bf79cd0fba6b6c7497df4ebf7d40970e2d7eceb8d0ea9c/setuptools-51.0.0-py3-none-any.whl"

WHEEL_PKG = "https://files.pythonhosted.org/packages/d4/cf/732e05dce1e37b63d54d1836160b6e24fb36eeff2313e93315ad047c7d90/wheel-0.36.1.tar.gz"

CHARSET_PKG = "https://files.pythonhosted.org/packages/84/3e/1037abe6498e65d645ce7a22d3402605d49a3b2c7f20c3abb027760da4f0/charset_normalizer-2.0.10-py3-none-any.whl"

IDNA_PKG = "https://files.pythonhosted.org/packages/04/a2/d918dcd22354d8958fe113e1a3630137e0fc8b44859ade3063982eacd2a4/idna-3.3-py3-none-any.whl"

CERTIFI_PKG = "https://files.pythonhosted.org/packages/37/45/946c02767aabb873146011e665728b680884cd8fe70dde973c640e45b775/certifi-2021.10.8-py2.py3-none-any.whl"

URLLIB3_PKG = "https://files.pythonhosted.org/packages/4e/b8/f5a25b22e803f0578e668daa33ba3701bb37858ec80e08a150bd7d2cf1b1/urllib3-1.26.8-py2.py3-none-any.whl"

http_file(
    name = "urllib3_pkg",
    downloaded_file_path = "urllib3-1.26.8-py2.py3-none-any.whl",
    sha256 = "000ca7f471a233c2251c6c7023ee85305721bfdf18621ebff4fd17a8653427ed",
    urls = [URLLIB3_PKG],
)

http_file(
    name = "certifi_pkg",
    downloaded_file_path = "certifi-2021.10.8-py2.py3-none-any.whl",
    sha256 = "d62a0163eb4c2344ac042ab2bdf75399a71a2d8c7d47eac2e2ee91b9d6339569",
    urls = [CERTIFI_PKG],
)

http_file(
    name = "idna_pkg",
    downloaded_file_path = "idna-3.3-py2.py3-none-any.whl",
    sha256 = "84d9dd047ffa80596e0f246e2eab0b391788b0503584e8945f2368256d2735ff",
    urls = [IDNA_PKG],
)

http_file(
    name = "charset_pkg",
    downloaded_file_path = "charset_normalizer-2.0.10-py3-none-any.whl",
    sha256 = "cb957888737fc0bbcd78e3df769addb41fd1ff8cf950dc9e7ad7793f1bf44455",
    urls = [CHARSET_PKG],
)

http_file(
    name = "pytest_pkg",
    downloaded_file_path = "pytest-6.2.5-py3-none-any.whl",
    sha256 = "7310f8d27bc79ced999e760ca304d69f6ba6c6649c0b60fb0e04a4a77cacc134",
    urls = [PYTEST_PKG],
)

http_file(
    name = "wheel_pkg",
    downloaded_file_path = "wheel-0.36.1.tar.gz",
    sha256 = "aaef9b8c36db72f8bf7f1e54f85f875c4d466819940863ca0b3f3f77f0a1646f",
    urls = [WHEEL_PKG],
)

http_file(
    name = "pex_pkg",
    downloaded_file_path = "pex-2.1.62-py2.py3-none-any.whl",
    sha256 = "7667c6c6d7a9b07c3ff3c3125c1928bd5279dfc077dd5cf4cc0440f40427c484",
    urls = [PEX_PKG],
)

http_file(
    name = "requests_pkg",
    downloaded_file_path = "requests-2.27.1-py2.py3-none-any.whl",
    sha256 = "f22fa1e554c9ddfd16e6e41ac79759e17be9e492b3587efa038054674760e72d",
    urls = [REQUESTS_PKG],
)

http_file(
    name = "setuptools_pkg",
    downloaded_file_path = "setuptools-51.0.0-py3-none-any.whl",
    sha256 = "8c177936215945c9a37ef809ada0fab365191952f7a123618432bbfac353c529",
    urls = [SETUPTOOLS_PKG],
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
    sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
    strip_prefix = "gflags-2.2.2",
    urls = ["https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"],
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
    sha256 = "90337653d92d4a13de590781371c604f9031cdb50520366aa1e3a91e1efb1017",
    strip_prefix = "libunwind-1.5.0",
    urls = ["https://github.com/libunwind/libunwind/releases/download/v1.5/libunwind-1.5.0.tar.gz"],
)

http_archive(
    name = "org_apache_zookeeper",
    build_file = "@//:third_party/zookeeper/BUILD",
    patch_args = ["-p1"],
    patches = ["//third_party/zookeeper:zookeeper_jute.patch"],
    sha256 = "1c52a4ea012c12c87e49298343eae44f89ce0d61133f7e07384d5fb64f8eaa77",
    strip_prefix = "apache-zookeeper-3.6.3",
    urls = ["https://downloads.apache.org/zookeeper/zookeeper-3.6.3/apache-zookeeper-3.6.3.tar.gz"],
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
    sha256 = "21bc744fb7f2fa701ee8db339ded7dce4f975d0d55837a97be7d46e8382dea5a",
    strip_prefix = "glog-0.5.0",
    urls = ["https://github.com/google/glog/archive/v0.5.0.zip"],
)

http_archive(
    name = "com_google_googletest",
    sha256 = "b4870bf121ff7795ba20d20bcdd8627b8e088f2d1dab299a031c1034eddc93d5",
    strip_prefix = "googletest-release-1.11.0",
    urls = ["https://github.com/google/googletest/archive/release-1.11.0.tar.gz"],
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
    sha256 = "5a0738afb1e194853aab00258453be8624e0a1d34fcc3c779989ac8dbcd59436",
    strip_prefix = "darwin-amd64",
    urls = ["https://get.helm.sh/helm-v3.7.2-darwin-amd64.tar.gz"],
)

http_archive(
    name = "helm_linux",
    build_file = "@//:third_party/helm/helm.BUILD",
    sha256 = "4ae30e48966aba5f807a4e140dad6736ee1a392940101e4d79ffb4ee86200a9e",
    strip_prefix = "linux-amd64",
    urls = ["https://get.helm.sh/helm-v3.7.2-linux-amd64.tar.gz"],
)
# end helm

# for docker image building

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "59536e6ae64359b716ba9c46c39183403b01eabfbd57578e84398b4829ca499a",
    strip_prefix = "rules_docker-0.22.0",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.22.0/rules_docker-v0.22.0.tar.gz"],
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
