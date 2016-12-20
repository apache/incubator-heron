licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "libunwind"
package_version = "1.1"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version
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
    srcs = [
        package_file,
        lzma_patch,
        config_patch,
        cache_patch
    ],
    outs = out_files,
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
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no --disable-minidebuginfo",
        "make install",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "libunwind-cxx",
    srcs = [
        "include/libunwind.h",
        "empty.cc",
        "lib/libunwind.a",
    ],
    hdrs = [
        "include/libunwind.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "libunwind-files",
    srcs = out_files,
)
