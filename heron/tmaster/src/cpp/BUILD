package(default_visibility = ["//visibility:public"])

cc_library(
    name = "tmaster-cxx",
    srcs = [
        "manager/stats-interface.cpp",
        "manager/stmgrstate.cpp",
        "manager/tcontroller.cpp",
        "manager/tmaster.cpp",
        "manager/tmasterserver.cpp",
        "manager/tmetrics-collector.cpp",

        "processor/stmgr-heartbeat-processor.cpp",
        "processor/stmgr-register-processor.cpp",
        "processor/tmaster-processor.cpp",

        "manager/stats-interface.h",
        "manager/stmgrstate.h",
        "manager/tcontroller.h",
        "manager/tmasterserver.h",
        "manager/tmetrics-collector.h",

        "processor/stmgr-heartbeat-processor.h",
        "processor/stmgr-register-processor.h",
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
    deps = [
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "//heron/proto:proto-cxx",
        "//third_party/yaml-cpp:yaml-cxx",
    ],
    linkstatic = 1,
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
    deps = [
        ":tmaster-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//config:config-cxx",
    ],
    linkstatic = 1,
)
