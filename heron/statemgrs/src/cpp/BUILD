package(default_visibility = ["//visibility:public"])

cc_library(
    name = "statemgrs-cxx",
    srcs = [
        "statemgr/heron-localfilestatemgr.cpp",
        "statemgr/heron-statemgr.cpp",
        "statemgr/heron-zkstatemgr.cpp",
    ],
    hdrs = [
        "statemgr/heron-statemgr.h",
        "statemgr/heron-localfilestatemgr.h",
        "statemgr/heron-zkstatemgr.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
    ],
    linkstatic = 1,
)
