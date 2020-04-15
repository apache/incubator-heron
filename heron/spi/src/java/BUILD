load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

package(default_visibility = ["//visibility:public"])

java_doc(
    name = "heron-spi-javadoc",
    libs = [":heron-spi"],
    pkgs = ["org/apache/heron/spi"],
    title = "Heron Spi Documentation",
)

java_library(
    name = "heron-spi",
    srcs = glob([
        "**/spi/common/**/*.java",
        "**/spi/packing/**/*.java",
        "**/spi/scheduler/**/*.java",
        "**/spi/statefulstorage/**/*.java",
        "**/spi/uploader/**/*.java",
        "**/spi/utils/**/*.java",
        "**/spi/statemgr/**/*.java",
        "**/spi/metricsmgr/**/*.java",
    ]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:utils-java",
        "//heron/proto:proto_ckptmgr_java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_packing_plan_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/proto:proto_topology_java",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:com_google_guava_guava",
        "@maven//:org_yaml_snakeyaml",
    ],
)

utils_deps_files = \
    heron_java_proto_files() + [
        ":common-spi-java",
        ":statemgr-spi-java",
        ":scheduler-spi-java",
        ":statefulstorage-spi-java",
        ":packing-spi-java",
        ":metricsmgr-spi-java",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:utils-java",
        "//heron/api/src/java:api-java-low-level",
        "@maven//:com_google_guava_guava",
    ]

scheduler_deps_files = \
    heron_java_proto_files() + [
        ":common-spi-java",
        ":packing-spi-java",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:config-java",
        "@maven//:com_google_guava_guava",
    ]

statefulstorage_deps_files = \
    heron_java_proto_files() + [
        ":common-spi-java",
        "//heron/common/src/java:config-java",
    ]

packing_deps_files = [
    "//heron/api/src/java:api-java-low-level",
    ":common-spi-java",
    "//heron/api/src/java:classification",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/proto:proto_packing_plan_java",
    "//heron/proto:proto_topology_java",
    "@maven//:com_google_guava_guava",
]

uploader_deps_files = [
    ":common-spi-java",
    "//heron/common/src/java:config-java",
]

statemgr_deps_files = \
    heron_java_proto_files() + [
        ":common-spi-java",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:config-java",
        "@maven//:com_google_guava_guava",
    ]

java_library(
    name = "common-spi-java",
    srcs = glob([
        "**/spi/common/**/*.java",
    ]),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "@maven//:com_google_guava_guava",
    ],
)

java_library(
    name = "utils-spi-java",
    srcs = glob([
        "**/spi/utils/**/*.java",
    ]),
    deps = utils_deps_files,
)

java_library(
    name = "packing-spi-java",
    srcs = glob([
        "**/spi/packing/**/*.java",
    ]),
    deps = packing_deps_files,
)

java_library(
    name = "scheduler-spi-java",
    srcs = glob([
        "**/spi/scheduler/**/*.java",
    ]),
    deps = scheduler_deps_files,
)

java_library(
    name = "statefulstorage-spi-java",
    srcs = glob([
        "**/spi/statefulstorage/**/*.java",
    ]),
    deps = statefulstorage_deps_files,
)

java_library(
    name = "shell-utils-java",
    srcs = glob([
        "**/spi/utils/ShellUtils.java",
    ]),
    deps = ["@maven//:com_google_guava_guava"],
)

java_library(
    name = "network-utils-java",
    srcs = glob([
        "**/spi/utils/NetworkUtils.java",
    ]),
    deps = [
        ":common-spi-java",
        ":shell-utils-java",
        "//heron/common/src/java:basics-java",
    ],
)

java_library(
    name = "uploader-spi-java",
    srcs = glob([
        "**/spi/uploader/**/*.java",
    ]),
    deps = uploader_deps_files,
)

java_library(
    name = "uploader-utils-java",
    srcs = glob([
        "**/spi/utils/UploaderUtils.java",
    ]),
    deps = uploader_deps_files,
)

java_library(
    name = "metricsmgr-spi-java",
    srcs = glob(
        ["**/spi/metricsmgr/**/*.java"],
    ),
)

java_library(
    name = "statemgr-spi-java",
    srcs = glob(
        ["**/spi/statemgr/**/*.java"],
    ),
    deps = statemgr_deps_files,
)

java_binary(
    name = "spi-unshaded",
    srcs = glob([
        "**/spi/common/**/*.java",
        "**/spi/metricsmgr/**/*.java",
        "**/spi/packing/**/*.java",
        "**/spi/scheduler/**/*.java",
        "**/spi/statefulstorage/**/*.java",
        "**/spi/statemgr/**/*.java",
        "**/spi/uploader/**/*.java",
        "**/spi/utils/**/*.java",
    ]),
    deps = [
        "//heron/api/src/java:classification",
    ] + utils_deps_files,
)

genrule(
    name = "heron-spi-jar",
    srcs = [":spi-unshaded_deploy.jar"],
    outs = ["heron-spi.jar"],
    cmd = "cp $< $@",
)
