load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "statemgrs-cxx",
    srcs = [
        "statemgr/heron-localfilestatemgr.cpp",
        "statemgr/heron-statemgr.cpp",
        "statemgr/heron-zkstatemgr.cpp",
    ],
    hdrs = [
        "statemgr/heron-localfilestatemgr.h",
        "statemgr/heron-statemgr.h",
        "statemgr/heron-zkstatemgr.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//config:config-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/proto:proto-cxx",
    ],
)
