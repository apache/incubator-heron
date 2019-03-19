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
    cmd  = "cp $(SRCS) $$(pwd)/$(@D)",
)

pex_library(
    name = "proto-py",
    srcs = glob(["**/*.py"]),
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
        ":proto_ckptmgr_py",
    ],
    reqs = [
        "protobuf==3.6.1",
        "setuptools==18.0.1",
    ],
)

pex_binary(
    name = "proto-py-package",
    srcs = glob(["**/*.py"]),
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
        ":proto_ckptmgr_py",
    ],
    reqs = [
        "protobuf==3.6.1",
        "setuptools==18.0.1",
    ],
)

proto_library(
    name = "proto_common",
    src = "common.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
)

proto_library(
    name = "proto_execution_state",
    src = "execution_state.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
)

proto_library(
    name = "proto_networktests",
    src = "networktests.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
)

proto_library(
    name = "proto_stats",
    src = "stats.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
)

proto_library(
    name = "proto_topology",
    src = "topology.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
)

proto_library(
    name = "proto_scheduler",
    src = "scheduler.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
    deps = [
        ":proto_common",
        ":proto_packing_plan"
    ],
)

proto_library(
    name = "proto_physical_plan",
    src = "physical_plan.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_packing_plan",
    src = "packing_plan.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
)

proto_library(
    name = "proto_tuple",
    src = "tuple.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
    deps = [":proto_topology"],
)

proto_library(
    name = "proto_tmaster",
    src = "tmaster.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
    deps = [
        ":proto_common",
        ":proto_physical_plan",
        ":proto_stats",
        ":proto_topology",
        ":proto_ckptmgr",
    ],
)

proto_library(
    name = "proto_metrics",
    src = "metrics.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
    deps = [
      ":proto_common",
      ":proto_tmaster",
    ],
)

proto_library(
    name = "proto_stmgr",
    src = "stmgr.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
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

proto_library(
    name = "proto_ckptmgr",
    src = "ckptmgr.proto",
    includes = ["$(GENDIR)/heronpy/proto"],
    gen_py = 1,
    deps = [
        ":proto_common",
        ":proto_physical_plan",
    ],
)
