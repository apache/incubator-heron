package(default_visibility = ["//visibility:public"])

cc_library(
    name = "grouping-cxx",
    srcs = [
        "grouping/all-grouping.cpp",
        "grouping/custom-grouping.cpp",
        "grouping/fields-grouping.cpp",
        "grouping/grouping.cpp",
        "grouping/lowest-grouping.cpp",
        "grouping/shuffle-grouping.cpp",
    ],
    hdrs = [
        "grouping/grouping.h",
        "grouping/all-grouping.h",
        "grouping/custom-grouping.h",
        "grouping/fields-grouping.h",
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
    deps = [
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
    ],
    linkstatic = 1,
)

cc_library(
    name = "util-cxx",
    srcs = [
        "util/rotating-map.cpp",
        "util/tuple-cache.cpp",
        "util/xor-manager.cpp",

        "util/rotating-map.h",
        "util/tuple-cache.h",
        "util/xor-manager.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/config:config-cxx",
    ],
    linkstatic = 1,
)


cc_library(
    name = "manager-cxx",
    srcs = [
        "manager/stmgr-client.cpp",
        "manager/stmgr-clientmgr.cpp",
        "manager/stmgr-server.cpp",
        "manager/stmgr.cpp",
        "manager/stream-consumers.cpp",
        "manager/tmaster-client.cpp",

        "manager/stmgr-client.h",
        "manager/stmgr-clientmgr.h",
        "manager/stmgr-server.h",
        "manager/stmgr.h",
        "manager/stream-consumers.h",
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
    deps = [
        ":grouping-cxx",
        ":util-cxx",
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "//third_party/yaml-cpp:yaml-cxx",
    ],
    linkstatic = 1,
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
    deps = [
        ":manager-cxx",
        ":grouping-cxx",
        ":util-cxx",
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "//third_party/yaml-cpp:yaml-cxx",
    ],
    linkstatic = 1,
)
