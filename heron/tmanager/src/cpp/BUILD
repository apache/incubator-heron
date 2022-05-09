load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "tmanager-cxx",
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
        "manager/tmanager.cpp",
        "manager/tmanagerserver.cpp",
        "manager/tmanagerserver.h",
        "manager/tmetrics-collector.cpp",
        "manager/tmetrics-collector.h",
        "processor/stmgr-heartbeat-processor.cpp",
        "processor/stmgr-heartbeat-processor.h",
        "processor/stmgr-register-processor.cpp",
        "processor/stmgr-register-processor.h",
        "processor/tmanager-processor.cpp",
        "processor/tmanager-processor.h",
    ],
    hdrs = [
        "manager/tmanager.h",
        "processor/processor.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/tmanager/src/cpp",
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
    name = "heron-tmanager",
    srcs = [
        "server/tmanager-main.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/tmanager/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":tmanager-cxx",
        "//config:config-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/proto:proto-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
    ],
)
