package(default_visibility = ["//visibility:public"])

error_file = "thread-errors.dat"

genrule(
    name = "gen-errs",
    srcs = [
        error_file,
    ],
    outs = [
        "thr-einfo-bakw-gen.h",
        "thr-einfo-gen.h",
        "thr-error-enum-gen.h",
    ],
    cmd = "\n".join([
        "export WORKSPACE_ROOT=$$(pwd)",
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t threads.XXXXX)",
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

cc_library(
    name = "threads-cxx",
    srcs = [
        "modinit.cpp",
        "pcqueue.cpp",
        "spcountdownlatch.cpp",

        "thr-einfo-bakw-gen.h",
        "thr-einfo-gen.h",
        "thr-error-enum-gen.h",

        "pcqueue.h",
        "spcountdownlatch.h",
    ],
    hdrs = [ 
        "threads.h",
        "modinit.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        "//heron/common/src/cpp/errors:errors-cxx",
        "//heron/common/src/cpp/basics:basics-cxx",
    ],
    linkstatic = 1,
)
