
load("/tools/rules/heron_deps", "heron_java_proto_files")

test_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-java",
        "@bazel_tools//third_party:junit4",
    ]

java_test(
    name = "custom-grouping_unittest",
    srcs = glob(
        ["**/CustomGroupingTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "bolt-instance_unittest",
    srcs = glob(
        ["**/BoltInstanceTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "activate-deactivate_unittest",
    srcs = glob(
        ["**/ActivateDeactivateTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "spout-instance_unittest",
    srcs = glob(
        ["**/SpoutInstanceTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "global-metrics_unittest",
    srcs = glob(
        ["**/GlobalMetricsTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "multiassignable-metric_unittest",
    srcs = glob(
        ["**/MultiAssignableMetricTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "connect_unittest",
    srcs = glob(
        ["**/ConnectTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "handle-read_unittest",
    srcs = glob(
        ["**/HandleReadTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "handle-write_unittest",
    srcs = glob(
        ["**/HandleWriteTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)
