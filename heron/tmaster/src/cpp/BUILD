load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "tmaster-cxx",
    srcs = [
        "manager/ckptmgr-client.cpp",
        "manager/ckptmgr-client.h",
        "manager/stateful-checkpointer.cpp",
        "manager/stateful-checkpointer.h",
        "manager/stateful-controller.cpp",
        "manager/stateful-controller.h",
        "manager/stateful-restorer.cpp",
        "manager/stateful-restorer.h",
        "manager/stats-interface.cpp",
        "manager/stats-interface.h",
        "manager/stmgrstate.cpp",
        "manager/stmgrstate.h",
        "manager/tcontroller.cpp",
        "manager/tcontroller.h",
        "manager/tmaster.cpp",
        "manager/tmasterserver.cpp",
        "manager/tmasterserver.h",
        "manager/tmetrics-collector.cpp",
        "manager/tmetrics-collector.h",
        "processor/stmgr-heartbeat-processor.cpp",
        "processor/stmgr-heartbeat-processor.h",
        "processor/stmgr-register-processor.cpp",
        "processor/stmgr-register-processor.h",
        "processor/tmaster-processor.cpp",
        "processor/tmaster-processor.h",
    ],
    hdrs = [
        "manager/tmaster.h",
        "processor/processor.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/tmaster/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/proto:proto-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "@com_github_cereal//:cereal-cxx",
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
    ],
)

cc_binary(
    name = "heron-tmaster",
    srcs = [
        "server/tmaster-main.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/tmaster/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":tmaster-cxx",
        "//config:config-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/proto:proto-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
    ],
)
