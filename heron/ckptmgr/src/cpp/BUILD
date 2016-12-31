package(default_visibility = ["//visibility:public"])

cc_library(
    name = "common-cxx",
    srcs = glob(["common/*.cpp"]),
    hdrs = glob(["common/*.h"]),
    copts = [
        "-Ithird_party",
        "-Iheron/ckptmgr/src/cpp",
    ],
    linkstatic = 1,
)

cc_library(
    name = "client-cxx",
    srcs = [
        "ckptclient/ckptmgr-client.cpp",
    ],
    hdrs = [
        "ckptclient/ckptmgr-client.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp"
    ],
    deps = [
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/network:network-cxx",
    ],
    linkstatic = 1,
)

cc_library(
    name = "manager-cxx",
    srcs = [
        "ckptmanager/ckptmgr-server.cpp",
        "ckptmanager/ckptmgr.cpp",
    ],
    hdrs = [
        "ckptmanager/ckptmgr-server.h",
        "ckptmanager/ckptmgr.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp"
    ],
    deps = [
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//third_party/yaml-cpp:yaml-cxx",
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
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//third_party/yaml-cpp:yaml-cxx",
    ],
    linkstatic = 1,
)

