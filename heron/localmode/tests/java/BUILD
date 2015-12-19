java_test(
    name = "instance_executor_unittest",
    srcs = glob(
        ["**/InstanceExecutorTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java",
        "//heron/localmode/src/java:localmode-lib-java",
        "//heron/proto:proto_physical_plan_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "all_grouping_unittest",
    srcs = glob(["**/AllGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto-java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "custom_grouping_unittest",
    srcs = glob(["**/CustomGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "fields_grouping_unittest",
    srcs = glob(["**/FieldsGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "lowest_grouping_unittest",
    srcs = glob(["**/LowestGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "shuffle_grouping_unittest",
    srcs = glob(["**/ShuffleGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "physical_plan_util_unittest",
    srcs = glob(["**/PhysicalPlanUtilTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto_physical_plan_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "rotating_map_unittest",
    srcs = glob(["**/RotatingMapTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "stream_consumers_unittest",
    srcs = glob(
        ["**/StreamConsumersTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto_physical_plan_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "tuple_cache_unittest",
    srcs = glob(["**/TupleCacheTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto_tuple_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)

java_test(
    name = "xor_manager_unittest",
    srcs = glob(
        ["**/XORManagerTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/proto:proto_physical_plan_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4", 
    ],
    size = "small",
)
