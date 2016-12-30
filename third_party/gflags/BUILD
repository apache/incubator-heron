licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "gflags"
package_version = "2.0"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [
    "include/gflags/gflags.h",
    "include/gflags/gflags_completions.h",
    "include/gflags/gflags_declare.h",
    "include/google/gflags.h",
    "include/google/gflags_completions.h",
]

lib_files = [
    "lib/libgflags.a",
    "lib/libgflags_nothreads.a",
]

genrule(
    name = "gflags-srcs",
    srcs = [package_file],
    outs = include_files + lib_files,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t gflags.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'cd ' + package_dir,
        './configure --prefix=$$INSTALL_DIR --enable-shared=no',
        'make install',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "gflags-cxx",
    srcs = [
        "empty.cc",
        "lib/libgflags.a",
    ],
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "gflags",
    srcs = [
        ":gflags-cxx",
    ]
)

filegroup(
    name = "gflags-files",
    srcs = include_files + lib_files,
)
