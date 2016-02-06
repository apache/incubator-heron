java_test(
    name = "custom_grouping_unittest",
    srcs = glob(
        ["**/CustomGroupingTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java", 
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java", 
        "//heron/proto:proto_stmgr_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "bolt_instance_unittest",
    srcs = glob(
        ["**/BoltInstanceTest.java"] +
        ["**/resource/*.java"]
    ), 
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "activate_deactivate_unittest",
    srcs = glob(
        ["**/ActivateDeactivateTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java", 
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "spout_instance_unittest",
    srcs = glob(
        ["**/SpoutInstanceTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java", 
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "global_metrics_unittest",
    srcs = glob(
        ["**/GlobalMetricsTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java", 
        "//heron/proto:proto_physical_plan_java", 
        "//heron/proto:proto_stmgr_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "multiassignable_metric_unittest",
    srcs = glob(
        ["**/MultiAssignableMetricTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "connect_unittest",
    srcs = glob(
        ["**/ConnectTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "handle_read_unittest",
    srcs = glob(
        ["**/HandleReadTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java", 
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)

java_test(
    name = "handle_write_unittest",
    srcs = glob(
        ["**/HandleWriteTest.java"] +
        ["**/resource/*.java"]
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/instance/src/java:instance-lib-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java", 
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    data = ["//heron/config/src/yaml:config-internals-yaml"],
    size = "small",
)
