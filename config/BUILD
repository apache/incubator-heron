load("@rules_cc//cc:defs.bzl", "cc_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

genrule(
    name = "config-srcs",
    srcs = glob(["m4/**/*.m4"]) + [
        "autogen.sh",
        "configure.ac",
        "heron.def",
    ],
    outs = [
        "heron-config.h",
    ],
    cmd = "\n".join([
        "export WORKSPACE_ROOT=$$(pwd)",
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t config.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$WORKSPACE_ROOT",
        "cd $$TMP_DIR",
        "touch Makefile.am",
        "mkdir -p m4",
        "mv *.m4 m4",
        "./autogen.sh",
        "./configure --enable-shared=no",
        "cp heron-config.h $$INSTALL_DIR",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "config-cxx",
    srcs = ["empty.cc"],
    hdrs = [
        "heron-config.h",
    ],
    linkstatic = 1,
)
