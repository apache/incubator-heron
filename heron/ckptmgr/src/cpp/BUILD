package(default_visibility = ["//visibility:public"])


cc_library(
    name = "manager-cxx",
    srcs = [
        "manager/ckptmgr-client.cpp",
        "manager/ckptmgr-server.cpp",
        "manager/ckptmgr.cpp",
    ],
    hdrs = [
        "manager/ckptmgr-client.h",
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
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/config:config-cxx",
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