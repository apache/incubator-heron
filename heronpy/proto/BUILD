load("//tools/rules:genproto.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

genrule(
    name = "protobuf-srcs",
    srcs = ["//heron/proto:proto-srcs"],
    outs = [
        "common.proto",
        "ckptmgr.proto",
        "execution_state.proto",
        "networktests.proto",
        "physical_plan.proto",
        "stats.proto",
        "tmaster.proto",
        "tuple.proto",
        "metrics.proto",
        "packing_plan.proto",
        "scheduler.proto",
        "stmgr.proto",
        "topology.proto",
    ],
    cmd = "cp $(SRCS) $$(pwd)/$(@D)",
)

pex_library(
    name = "proto-py",
    srcs = glob(["**/*.py"]),
    reqs = [
        "protobuf==3.8.0",
        "setuptools==18.8.1",
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
        ":proto_tmaster_py",
        ":proto_topology_py",
        ":proto_tuple_py",
    ],
)

pex_binary(
    name = "proto-py-package",
    srcs = glob(["**/*.py"]),
    reqs = [
        "protobuf==3.8.0",
        "setuptools==18.8.1",
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
        ":proto_tmaster_py",
        ":proto_topology_py",
        ":proto_tuple_py",
    ],
)

proto_library(
    name = "proto_common",
    src = "common.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
)

proto_library(
    name = "proto_execution_state",
    src = "execution_state.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
)

proto_library(
    name = "proto_networktests",
    src = "networktests.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
)

proto_library(
    name = "proto_stats",
    src = "stats.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
)

proto_library(
    name = "proto_topology",
    src = "topology.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
)

proto_library(
    name = "proto_scheduler",
    src = "scheduler.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
    deps = [
        ":proto_common",
        ":proto_packing_plan",
    ],
)

proto_library(
    name = "proto_physical_plan",
    src = "physical_plan.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_packing_plan",
    src = "packing_plan.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
)

proto_library(
    name = "proto_tuple",
    src = "tuple.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_tmaster",
    src = "tmaster.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
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
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
    deps = [
        ":proto_common",
        ":proto_tmaster",
    ],
)

proto_library(
    name = "proto_stmgr",
    src = "stmgr.proto",
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
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
    gen_py = 1,
    includes = ["$(GENDIR)/heronpy/proto"],
    deps = [
        ":proto_common",
        ":proto_physical_plan",
    ],
)
