load("/tools/rules/heron", "heron_java_proto_files")

java_test(
    name = "instance-executor_unittest",
    srcs = glob(
        ["**/InstanceExecutorTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java",
        "//heron/localmode/src/java:localmode-lib-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "all-grouping_unittest",
    srcs = glob(["**/AllGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "custom-grouping_unittest",
    srcs = glob(["**/CustomGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "fields-grouping_unittest",
    srcs = glob(["**/FieldsGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "//heron/api/src/java:api-java",
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "lowest-grouping_unittest",
    srcs = glob(["**/LowestGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "shuffle-grouping_unittest",
    srcs = glob(["**/ShuffleGroupingTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "physical-plan-util_unittest",
    srcs = glob(["**/PhysicalPlanUtilTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "rotating-map_unittest",
    srcs = glob(["**/RotatingMapTest.java"]),
    deps = [
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "stream-consumers_unittest",
    srcs = glob(
        ["**/StreamConsumersTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "tuple-cache_unittest",
    srcs = glob(["**/TupleCacheTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)

java_test(
    name = "xor-manager_unittest",
    srcs = glob(
        ["**/XORManagerTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/localmode/src/java:localmode-lib-java", 
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4", 
    ] + heron_java_proto_files(),

    size = "small",
)
