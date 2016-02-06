package(default_visibility = ["//visibility:public"])

java_library(
    name = "state-java",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//heron/common/src/java:base-java",
        "//heron/proto:proto-java",
        "//heron/proto:proto_execution_state_java", 
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/proto:proto_topology_java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/logging:logging-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ],
)
