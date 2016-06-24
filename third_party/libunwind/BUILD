licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "libunwind"
package_version = "1.1"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

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
    ],
    outs = out_files,
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t libunwind.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "tar xfz " + package_file,
        "cd " + package_dir,
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no",
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
