package(default_visibility = ["//visibility:public"])

cc_library(
    name = "common-cxx",
    srcs = glob(["common/*.cpp"]),
    hdrs = glob(["common/*.h"]),
    copts = [
        "-Iheron",
        "-I$(GENDIR)/heron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
    ],
    deps = [
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/network:network-cxx",
    ],
    linkstatic = 1,
)

cc_library(
    name = "lfs-cxx",
    srcs = glob(["lfs/*.cpp"]),
    hdrs = glob(["lfs/*.h"]),
    copts = [
        "-Iheron",
        "-I$(GENDIR)/heron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
    ],
    deps = [
        ":common-cxx",
    ],
    linkstatic = 1,
)

cc_library(
    name = "client-cxx",
    srcs = [
        "client/ckptmgr-client.cpp",
    ],
    hdrs = [
        "client/ckptmgr-client.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp"
    ],
    deps = [
        ":common-cxx",
    ],
    linkstatic = 1,
)

cc_library(
    name = "manager-cxx",
    srcs = [
        "manager/ckptmgr-server.cpp",
        "manager/ckptmgr.cpp",
    ],
    hdrs = [
        "manager/ckptmgr-server.h",
        "manager/ckptmgr.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp"
    ],
    deps = [
        ":common-cxx",
    ],
    linkstatic = 1,
)

cc_binary(
    name = "heron-ckptmgr",
    srcs = [
        "server/ckptmgr-main.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        ":manager-cxx",
    ],
    linkstatic = 1,
)

