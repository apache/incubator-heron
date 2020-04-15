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

load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

package(default_visibility = ["//visibility:public"])

storm_deps_files = [
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/simulator/src/java:simulator-java",
    "//heron/proto:proto_topology_java",
    "@maven//:com_googlecode_json_simple_json_simple",
    "//third_party/java:kryo-neverlink",
]

java_doc(
    name = "heron-storm-javadoc",
    libs = [":storm-compatibility-java"],
    pkgs = [
        "backtype",
        "clojure",
        "org",
        "storm",
    ],
    title = "Heron Storm Compatibility Api Documentation",
)

java_library(
    name = "storm-compatibility-java-neverlink",
    srcs = glob(["**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    neverlink = 1,
    deps = storm_deps_files,
)

# Kryo is bundled here for integration test
java_library(
    name = "storm-compatibility-java",
    srcs = glob(["**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = storm_deps_files + [
        "//eco/src/java:eco-java",
        "//third_party/java:kryo",
    ],
)

java_binary(
    name = "storm-compatibility-unshaded",
    srcs = glob(["**/*.java"]),
    deps = storm_deps_files + [
        "//eco/src/java:eco-java",
    ],
)

jarjar_binary(
    name = "heron-storm",
    src = ":storm-compatibility-unshaded_deploy.jar",
    shade = "shade.conf",
    deps = ["@maven//:org_sonatype_plugins_jarjar_maven_plugin"],
)
