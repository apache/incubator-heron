licenses(["notice"])

package(default_visibility = ["//visibility:public"])

out_files = [
    "include/libunwind-common.h",
    "include/libunwind-coredump.h",
    "include/libunwind-dynamic.h",
    "include/libunwind.h",
    "include/libunwind-ptrace.h",
    "include/libunwind-x86_64.h",
    "include/unwind.h",
    "lib/libunwind.a",
    "lib/libunwind-coredump.a",
    "lib/libunwind-ptrace.a",
    "lib/libunwind-setjmp.a",
    "lib/libunwind-x86_64.a",
]

genrule(
    name = "libunwind-srcs",
    outs = out_files,
    cmd = "\n".join([
        "export SOURCE_DIR=$$(pwd)",
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t libunwind.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -LR $$(pwd)/external/org_nongnu_libunwind/* $$TMP_DIR",
        "cd $$TMP_DIR",
        "autoreconf -f -i",
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no --disable-minidebuginfo",
        'make install SUBDIRS="src tests"',
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "libunwind",
    srcs = [
        "include/libunwind.h",
        "lib/libunwind.a",
    ],
    hdrs = ["include/libunwind.h"],
    includes = ["include"],
    linkstatic = 1,
)

filegroup(
    name = "libunwind-files",
    srcs = out_files,
)
