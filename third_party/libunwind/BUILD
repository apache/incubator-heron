licenses(["notice"])

package(default_visibility = ["//visibility:public"])

pkg_name = "libunwind"
pkg_version = "1.1"

package_file = pkg_name + "-" + pkg_version + ".tar.gz"
package_dir = pkg_name + "-" + pkg_version
lzma_patch = "libunwind-1.1-lzma-link.patch"
config_patch = "libunwind-1.1-config.patch"
cache_patch = "libunwind-1.1-cache.patch"


include_files = [
    "include/libunwind-common.h",
    "include/libunwind-coredump.h",
    "include/libunwind-dynamic.h",
    "include/libunwind.h",
    "include/libunwind-ptrace.h",
    "include/libunwind-x86_64.h",
    "include/unwind.h",
]

lib_static_files = [
    "lib/libunwind.a",
    "lib/libunwind-coredump.a",
    "lib/libunwind-ptrace.a",
    "lib/libunwind-setjmp.a",
    "lib/libunwind-x86_64.a",
]

lib_dynamic_files = [
    "lib/libunwind.so.8.0.1",
    "lib/libunwind-coredump.so.0.0.0",
    "lib/libunwind-ptrace.so.0.0.0",
    "lib/libunwind-setjmp.so.0.0.0",
    "lib/libunwind-x86_64.so.8.0.1",
]

genrule(
    name = "libunwind-srcs",
    srcs = [
        package_file,
        lzma_patch,
        config_patch,
        cache_patch
    ],
    outs = include_files + lib_static_files + lib_dynamic_files,
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t libunwind.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "tar xfz " + package_file,
        "patch -d " + package_dir + " -p1 < " + lzma_patch,
        "patch -d " + package_dir + " -p0 < " + config_patch,
        "patch -d " + package_dir + " -p0 < " + cache_patch,
        "cd " + package_dir,
        "./configure --prefix=$$INSTALL_DIR --disable-minidebuginfo",
        "make install",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "libunwind-cxx",
    srcs = [
        "include/libunwind.h",
        "empty.cc",
    ] + lib_static_files,
    hdrs = [
        "include/libunwind.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)

cc_library(
    name = "libunwind-shared-cxx",
    srcs = [
        "include/libunwind.h",
        "empty.cc",
    ] + lib_dynamic_files,
    hdrs = [
        "include/libunwind.h",
    ],
    includes = [
        "include",
    ],
)


filegroup(
    name = "libunwind-files",
    srcs = include_files + lib_static_files,
)

filegroup(
    name = "libunwind-dynamic-files",
    srcs = lib_dynamic_files, 
)
