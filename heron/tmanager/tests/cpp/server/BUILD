load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "tmanager_unittest",
    size = "small",
    srcs = [
        "dummystmgr.cpp",
        "dummystmgr.h",
        "tmanager_unittest.cpp",
    ],
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-Iheron/tmanager/src/cpp",
        "-Iheron/tmanager/tests/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    data = [
        "//heron/config/src/yaml:test-config-internals-yaml",
    ],
    flaky = 1,
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/tmanager/src/cpp:tmanager-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "stateful_checkpointer_unittest",
    size = "small",
    srcs = [
        "stateful_checkpointer_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-Iheron/tmanager/src/cpp",
        "-Iheron/tmanager/tests/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    flaky = 1,
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/tmanager/src/cpp:tmanager-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "stateful_restorer_unittest",
    size = "small",
    srcs = [
        "dummystmgr.cpp",
        "dummystmgr.h",
        "dummytmanager.cpp",
        "dummytmanager.h",
        "stateful_restorer_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/tmanager/src/cpp",
        "-Iheron/tmanager/tests/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    flaky = 1,
    linkstatic = 1,
    deps = [
        "//heron/tmanager/src/cpp:tmanager-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "tcontroller_unittest",
    size = "small",
    srcs = [
        "tcontroller_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/tmanager/src/cpp",
        "-Iheron/tmanager/tests/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    flaky = 0,
    linkstatic = 1,
    deps = [
        "//heron/tmanager/src/cpp:tmanager-cxx",
        "@com_google_googletest//:gtest",
    ],
)
