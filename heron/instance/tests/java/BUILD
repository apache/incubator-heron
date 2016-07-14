
load("/tools/rules/heron_deps", "heron_java_proto_files")

test_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-java",
        "//third_party/java:junit4",
    ]

java_test(
    name = "CustomGroupingTest",
    srcs = glob(
        ["**/CustomGroupingTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "BoltInstanceTest",
    srcs = glob(
        ["**/BoltInstanceTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "ActivateDeactivateTest",
    srcs = glob(
        ["**/ActivateDeactivateTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "SpoutInstanceTest",
    srcs = glob(
        ["**/SpoutInstanceTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "GlobalMetricsTest",
    srcs = glob(
        ["**/GlobalMetricsTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "MultiAssignableMetricTest",
    srcs = glob(
        ["**/MultiAssignableMetricTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "ConnectTest",
    srcs = glob(
        ["**/ConnectTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "HandleReadTest",
    srcs = glob(
        ["**/HandleReadTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)

java_test(
    name = "HandleWriteTest",
    srcs = glob(
        ["**/HandleWriteTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = test_deps_files,
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)
