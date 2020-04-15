load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "grouping-cxx",
    srcs = [
        "grouping/all-grouping.cpp",
        "grouping/custom-grouping.cpp",
        "grouping/direct-grouping.cpp",
        "grouping/fields-grouping.cpp",
        "grouping/grouping.cpp",
        "grouping/lowest-grouping.cpp",
        "grouping/shuffle-grouping.cpp",
    ],
    hdrs = [
        "grouping/all-grouping.h",
        "grouping/custom-grouping.h",
        "grouping/direct-grouping.h",
        "grouping/fields-grouping.h",
        "grouping/grouping.h",
        "grouping/lowest-grouping.h",
        "grouping/shuffle-grouping.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//config:config-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/proto:proto-cxx",
    ],
)

cc_library(
    name = "util-cxx",
    srcs = [
        "util/neighbour-calculator.cpp",
        "util/neighbour-calculator.h",
        "util/rotating-map.cpp",
        "util/rotating-map.h",
        "util/tuple-cache.cpp",
        "util/tuple-cache.h",
        "util/xor-manager.cpp",
        "util/xor-manager.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//config:config-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/proto:proto-cxx",
        "@com_github_hopscotch_hashmap//:hopscotch-hashmap",
    ],
)

cc_library(
    name = "manager-cxx",
    srcs = [
        "manager/checkpoint-gateway.cpp",
        "manager/checkpoint-gateway.h",
        "manager/ckptmgr-client.cpp",
        "manager/ckptmgr-client.h",
        "manager/instance-server.cpp",
        "manager/instance-server.h",
        "manager/stateful-restorer.cpp",
        "manager/stateful-restorer.h",
        "manager/stmgr.cpp",
        "manager/stmgr.h",
        "manager/stmgr-client.cpp",
        "manager/stmgr-client.h",
        "manager/stmgr-clientmgr.cpp",
        "manager/stmgr-clientmgr.h",
        "manager/stmgr-server.cpp",
        "manager/stmgr-server.h",
        "manager/stream-consumers.cpp",
        "manager/stream-consumers.h",
        "manager/tmaster-client.cpp",
        "manager/tmaster-client.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":grouping-cxx",
        ":util-cxx",
        "//config:config-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/proto:proto-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
    ],
)

cc_binary(
    name = "heron-stmgr",
    srcs = [
        "server/stmgr-main.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":grouping-cxx",
        ":manager-cxx",
        ":util-cxx",
        "//config:config-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/proto:proto-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
    ],
)
