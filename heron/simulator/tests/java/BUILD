load("/tools/rules/heron_deps", "heron_java_proto_files")

test_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java",
        "//heron/simulator/src/java:simulator-java",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ]

java_test(
    name = "InstanceExecutorTest",
    srcs = glob(
        ["**/InstanceExecutorTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = test_deps_files,
    size = "small",
)

java_test(
    name = "AllGroupingTest",
    srcs = glob(["**/AllGroupingTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "CustomGroupingTest",
    srcs = glob(["**/CustomGroupingTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "FieldsGroupingTest",
    srcs = glob(["**/FieldsGroupingTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "LowestGroupingTest",
    srcs = glob(["**/LowestGroupingTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "ShuffleGroupingTest",
    srcs = glob(["**/ShuffleGroupingTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "PhysicalPlanUtilTest",
    srcs = glob(["**/PhysicalPlanUtilTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "RotatingMapTest",
    srcs = glob(["**/RotatingMapTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "StreamConsumersTest",
    srcs = glob(
        ["**/StreamConsumersTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "TupleCacheTest",
    srcs = glob(["**/TupleCacheTest.java"]),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "XORManagerTest",
    srcs = glob(
        ["**/XORManagerTest.java"] +
        ["**/PhysicalPlanUtilTest.java"],
    ),
    deps = test_deps_files, 
    size = "small",
)

java_test(
    name = "SimulatorTest",
    srcs = glob(["**/SimulatorTest.java"]),
    deps = test_deps_files,
    size = "small",
)
