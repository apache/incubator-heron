package(default_visibility = ["//visibility:public"])

cc_library(
    name = "statemgrs-cxx",
    srcs = [
        "heron-localfilestatemgr.cpp",
        "heron-statemgr.cpp",
        "heron-zkstatemgr.cpp",

        "heron-localfilestatemgr.h",
        "heron-zkstatemgr.h",
    ],
    hdrs = [
        "heron-statemgr.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
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
