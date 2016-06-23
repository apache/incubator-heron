package(default_visibility = ["//visibility:public"])

error_file = "errors.dat"

genrule(
    name = "gen-errs",
    srcs = [
        error_file,
    ],
    outs = [
        "testerrs-einfo-bakw-gen.h",
        "testerrs-einfo-gen.h",
        "testerrs-error-enum-gen.h",
    ],
    cmd = "\n".join([
        "export WORKSPACE_ROOT=$$(pwd)",
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t errors.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "mkdir -p $$INSTALL_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "$$WORKSPACE_ROOT/$(location //scripts/compile:gen_errcodes) -p -e " + error_file,
        "cp -r $$TMP_DIR/*.h $$INSTALL_DIR",
        "rm -rf $$TMP_DIR",
    ]),
    tools = [
        "//scripts/compile:gen_errcodes",
    ],
)

cc_test(
    name = "errors_unittest",
    srcs = [
        "errors_unittest.cpp",
        "testerrs-einfo-bakw-gen.h",
        "testerrs-einfo-gen.h",
        "testerrs-error-enum-gen.h",
    ],
    deps = [
        "//heron/common/src/cpp/errors:errors-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "module_unittest",
    srcs = [
        "module_unittest.cpp",
        "testerrs-einfo-bakw-gen.h",
        "testerrs-einfo-gen.h",
        "testerrs-error-enum-gen.h",
    ],
    deps = [
        "//heron/common/src/cpp/errors:errors-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "syserrs_unittest",
    srcs = [
        "syserrs_unittest.cpp",
        "testerrs-einfo-bakw-gen.h",
        "testerrs-einfo-gen.h",
        "testerrs-error-enum-gen.h",
    ],
    deps = [
        "//heron/common/src/cpp/errors:errors-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    size = "small",
    linkstatic = 1,
)
