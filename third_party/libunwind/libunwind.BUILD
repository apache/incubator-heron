licenses(["notice"])

package(default_visibility = ["//visibility:public"])

lzma_patch = "libunwind-1.1-lzma-link.patch"
config_patch = "libunwind-1.1-config.patch"
cache_patch = "libunwind-1.1-cache.patch"

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
        "export SOURCE_DIR=$$(pwd)/third_party/libunwind",
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t libunwind.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $$(pwd)/external/org_savannah_libunwind/* $$TMP_DIR",
        "cd $$TMP_DIR",
        "patch -d " + package_dir + " -p1 < $$(SOURCE_DIR)/" + lzma_patch,
        "patch -d " + package_dir + " -p0 < $$(SOURCE_DIR)/" + config_patch,
        "patch -d " + package_dir + " -p0 < $$(SOURCE_DIR)/" + cache_patch,
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no --disable-minidebuginfo",
        "make install",
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
