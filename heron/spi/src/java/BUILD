package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/heron_deps", "heron_java_proto_files")

java_library(
    name = 'heron-spi',
    srcs = glob([
        "**/spi/common/**/*.java",
        "**/spi/packing/**/*.java",
        "**/spi/scheduler/**/*.java",
        "**/spi/uploader/**/*.java",
        "**/spi/utils/**/*.java",
        "**/spi/statemgr/**/*.java",
        "**/spi/metricsmgr/**/*.java",
    ]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/proto:proto_topology_java",
        "//heron/proto:proto_packing_plan_java",
        "//heron/proto:proto_physical_plan_java",
        "@org_yaml_snakeyaml//jar",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
    ],
)

filegroup(
    name = "config-keys-yaml",
    srcs = glob(["**/spi/common/**/keys.yaml"]),
)

filegroup(
    name = "config-defaults-yaml",
    srcs = glob(["**/spi/common/**/defaults.yaml"]),
)

common_resource_files = [
    ":config-keys-yaml",
    ":config-defaults-yaml",
]

utils_deps_files = \
    heron_java_proto_files() + [
        ":common-spi-java",
        ":statemgr-spi-java",
        ":scheduler-spi-java",
        ":packing-spi-java",
        ":metricsmgr-spi-java",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/api/src/java:api-java",
        "@com_google_guava_guava//jar",
    ]

scheduler_deps_files = \
    heron_java_proto_files() + [
        ":common-spi-java",
        ":packing-spi-java",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:config-java",
        "@com_google_guava_guava//jar",
    ]

packing_deps_files = [
    "//heron/api/src/java:api-java",
    ":common-spi-java",
    "//heron/api/src/java:classification",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/proto:proto_packing_plan_java",
    "//heron/proto:proto_topology_java",
    "@com_google_guava_guava//jar",
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
        "@com_google_guava_guava//jar",
    ]

java_library(
    name='common-spi-java',
    srcs = glob([
        "**/spi/common/**/*.java",
    ]),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "@com_google_guava_guava//jar",
    ],
    resources = common_resource_files,
)

java_library(
    name='utils-spi-java',
    srcs = glob([
        "**/spi/utils/**/*.java",
    ]),
    deps = utils_deps_files,
)

java_library(
    name='packing-spi-java',
    srcs = glob([
        "**/spi/packing/**/*.java",
    ]),
    deps = packing_deps_files,
)

java_library(
    name='scheduler-spi-java',
    srcs = glob([
        "**/spi/scheduler/**/*.java",
    ]),
    deps = scheduler_deps_files,
)

java_library(
    name='shell-utils-java',
    srcs = glob([
        "**/spi/utils/ShellUtils.java",
    ]),
    deps = [ "@com_google_guava_guava//jar" ],
)

java_library(
    name='network-utils-java',
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
    name='uploader-spi-java',
    srcs = glob([
        "**/spi/uploader/**/*.java",
    ]),
    deps = uploader_deps_files,
)

java_library(
    name='uploader-utils-java',
    srcs = glob([
        "**/spi/utils/UploaderUtils.java",
    ]),
    deps = uploader_deps_files,
)

java_library(
    name='metricsmgr-spi-java',
    srcs = glob(
        ["**/spi/metricsmgr/**/*.java"],
    ),
)

java_library(
    name='statemgr-spi-java',
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
        "**/spi/statemgr/**/*.java",
        "**/spi/uploader/**/*.java",
        "**/spi/utils/**/*.java",
    ]),
    deps = [
        "//heron/api/src/java:classification"
    ] + utils_deps_files,
)

genrule(
    name = "heron-spi-jar",
    srcs = [":spi-unshaded_deploy.jar"],
    outs = ["heron-spi.jar"],
    cmd  = "cp $< $@",
)
