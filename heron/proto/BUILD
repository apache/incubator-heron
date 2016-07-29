package(default_visibility = ["//visibility:public"])

load("/tools/rules/genproto",  "proto_library")
load("/tools/rules/pex_rules", "pex_library")

proto_library(
    name = "proto_common",
    src = "common.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
)

proto_library(
    name = "proto_execution_state",
    src = "execution_state.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
)

proto_library(
    name = "proto_networktests",
    src = "networktests.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
)

proto_library(
    name = "proto_stats",
    src = "stats.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
)

proto_library(
    name = "proto_topology",
    src = "topology.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
)

proto_library(
    name = "proto_scheduler",
    src = "scheduler.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    deps = [
        ":proto_common",
        ":proto_packing_plan"
    ],
)

proto_library(
    name = "proto_physical_plan",
    src = "physical_plan.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_packing_plan",
    src = "packing_plan.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
)

proto_library(
    name = "proto_tuple",
    src = "tuple.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_tmaster",
    src = "tmaster.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    deps = [
        ":proto_common",
        ":proto_physical_plan",
        ":proto_stats",
        ":proto_topology",
    ],
)

proto_library(
    name = "proto_metrics",
    src = "metrics.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    deps = [
      ":proto_common",
      ":proto_tmaster",
    ],
)

proto_library(
    name = "proto_stmgr",
    src = "stmgr.proto",
    includes = ["heron/proto"],
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    deps = [
        ":proto_common",
        ":proto_packing_plan",
        ":proto_physical_plan",
        ":proto_stats",
        ":proto_topology",
        ":proto_tuple",
    ],
)

java_library(
    name = "proto-java",
    deps = [
        ":proto_common_java",
        ":proto_execution_state_java",
        ":proto_stats_java",
        ":proto_topology_java",
        ":proto_scheduler_java",
        ":proto_packing_plan_java",
        ":proto_physical_plan_java",
        ":proto_metrics_java",
        ":proto_tmaster_java",
        ":proto_tuple_java",
        ":proto_stmgr_java",
        ":proto_networktests_java",
        "@com_google_protobuf_protobuf_java//jar",
    ],
    srcs = [
        "Empty.java",
    ],
)

# This is for binaries that don't want protobuf 
# dependency to be included in the generated jar
java_library(
    name = "proto-java-nl",
    srcs = [
        "Empty.java",
    ],
    deps = [
        ":proto_common_java",
        ":proto_execution_state_java",
        ":proto_stats_java",
        ":proto_topology_java",
        ":proto_scheduler_java",
        ":proto_packing_plan_java",
        ":proto_physical_plan_java",
        ":proto_metrics_java",
        ":proto_tmaster_java",
        ":proto_tuple_java",
        ":proto_stmgr_java",
        ":proto_networktests_java",
        "//third_party/java:protobuf-java-neverlink",
    ],
)

java_binary(
    name = "proto-bin-java",
    srcs = ["Empty.java"],
    create_executable = 0,
    deps = [
        ":proto-java-nl",
    ],
)

pex_library(
    name = "proto-py",
    deps = [
        ":proto_common_py",
        ":proto_execution_state_py",
        ":proto_stats_py",
        ":proto_topology_py",
        ":proto_scheduler_py",
        ":proto_packing_plan_py",
        ":proto_physical_plan_py",
        ":proto_metrics_py",
        ":proto_tmaster_py",
        ":proto_tuple_py",
        ":proto_stmgr_py",
    ],
    reqs = [
        "protobuf==2.5.0",
        "setuptools==18.0.1",
    ],
)

cc_library(
    name = "proto-cxx",
    srcs = [
        "empty.cc",
    ],
    hdrs = [
        "messages.h",

        "common.pb.h",
        "execution_state.pb.h",
        "metrics.pb.h",
        "physical_plan.pb.h",
        "scheduler.pb.h",
        "stats.pb.h",
        "stmgr.pb.h",
        "tmaster.pb.h",
        "topology.pb.h",
        "tuple.pb.h",
    ],
    deps = [
        ":proto_metrics_cc",
        ":proto_stmgr_cc",
        ":proto_scheduler_cc",
        ":proto_tmaster_cc",
        ":proto_tuple_cc",
        ":proto_packing_plan_cc",
        ":proto_physical_plan_cc",
        ":proto_execution_state_cc",
        ":proto_topology_cc",
        ":proto_stats_cc",
        ":proto_common_cc",
        "//third_party/protobuf:protobuf-cxx",
    ],
    linkstatic = 1,
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
