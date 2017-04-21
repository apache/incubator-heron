package(default_visibility = ["//visibility:public"])

error_file = "sys-errors.dat"

genrule(
    name = "gen-errs",
    srcs = [
        error_file,
    ],
    outs = [
        "sys-einfo-bakw-gen.h",
        "sys-einfo-gen.h",
        "sys-error-enum-gen.h",
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

cc_library(
    name = "errors-cxx",
    srcs = [
        "gexception.cpp",
        "modinit.cpp",
        "sperrimpl.cpp",
        "sperror.cpp",
        "spexcept.cpp",

        "sys-einfo-bakw-gen.h",
        "sys-einfo-gen.h",
        "sys-error-enum-gen.h",
        "sperrimpl.h",

        "gexception.h",
        "sperrmod.h",
        "sperror.h",
        "spexcept.h",
        "syserr.h",
    ],
    hdrs = [ 
        "errors.h",
        "modinit.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        "//heron/common/src/cpp/basics:basics-cxx",
    ],
    linkstatic = 1,
)
