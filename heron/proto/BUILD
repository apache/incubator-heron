load("//tools/rules:genproto.bzl", "proto_library")
load("@rules_java//java:defs.bzl", "java_library")
load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "proto-srcs",
    srcs = glob(["**/*.proto"]),
)

proto_library(
    name = "proto_common",
    src = "common.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
)

proto_library(
    name = "proto_execution_state",
    src = "execution_state.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
)

proto_library(
    name = "proto_networktests",
    src = "networktests.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
)

proto_library(
    name = "proto_stats",
    src = "stats.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
)

proto_library(
    name = "proto_topology",
    src = "topology.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
)

proto_library(
    name = "proto_scheduler",
    src = "scheduler.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
    deps = [
        ":proto_common",
        ":proto_packing_plan",
    ],
)

proto_library(
    name = "proto_physical_plan",
    src = "physical_plan.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_packing_plan",
    src = "packing_plan.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
)

proto_library(
    name = "proto_tuple",
    src = "tuple.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_tmanager",
    src = "tmanager.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
    deps = [
        ":proto_ckptmgr",
        ":proto_common",
        ":proto_physical_plan",
        ":proto_stats",
        ":proto_topology",
    ],
)

proto_library(
    name = "proto_metrics",
    src = "metrics.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
    deps = [
        ":proto_common",
        ":proto_tmanager",
    ],
)

proto_library(
    name = "proto_stmgr",
    src = "stmgr.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
    deps = [
        ":proto_common",
        ":proto_packing_plan",
        ":proto_physical_plan",
        ":proto_stats",
        ":proto_topology",
        ":proto_tuple",
    ],
)

proto_library(
    name = "proto_ckptmgr",
    src = "ckptmgr.proto",
    gen_cc = 1,
    gen_java = 1,
    gen_py = 1,
    includes = ["heron/proto"],
    deps = [
        ":proto_common",
        ":proto_physical_plan",
    ],
)

java_library(
    name = "proto-java",
    srcs = [
        "Empty.java",
    ],
    deps = [
        ":proto_ckptmgr_java",
        ":proto_common_java",
        ":proto_execution_state_java",
        ":proto_metrics_java",
        ":proto_networktests_java",
        ":proto_packing_plan_java",
        ":proto_physical_plan_java",
        ":proto_scheduler_java",
        ":proto_stats_java",
        ":proto_stmgr_java",
        ":proto_tmanager_java",
        ":proto_topology_java",
        ":proto_tuple_java",
        "@com_google_protobuf//:protobuf_java",
    ],
)

pex_library(
    name = "proto-py",
    reqs = [
        "protobuf==3.14.0",
        "setuptools==51.0.0",
    ],
    deps = [
        ":proto_ckptmgr_py",
        ":proto_common_py",
        ":proto_execution_state_py",
        ":proto_metrics_py",
        ":proto_packing_plan_py",
        ":proto_physical_plan_py",
        ":proto_scheduler_py",
        ":proto_stats_py",
        ":proto_stmgr_py",
        ":proto_tmanager_py",
        ":proto_topology_py",
        ":proto_tuple_py",
    ],
)

cc_library(
    name = "proto-cxx",
    srcs = [
        "empty.cc",
    ],
    hdrs = [
        "ckptmgr.pb.h",
        "common.pb.h",
        "execution_state.pb.h",
        "messages.h",
        "metrics.pb.h",
        "physical_plan.pb.h",
        "scheduler.pb.h",
        "stats.pb.h",
        "stmgr.pb.h",
        "tmanager.pb.h",
        "topology.pb.h",
        "tuple.pb.h",
    ],
    linkstatic = 1,
    deps = [
        ":proto_ckptmgr_cc",
        ":proto_common_cc",
        ":proto_execution_state_cc",
        ":proto_metrics_cc",
        ":proto_packing_plan_cc",
        ":proto_physical_plan_cc",
        ":proto_scheduler_cc",
        ":proto_stats_cc",
        ":proto_stmgr_cc",
        ":proto_tmanager_cc",
        ":proto_topology_cc",
        ":proto_tuple_cc",
        "@com_google_protobuf//:protobuf",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
