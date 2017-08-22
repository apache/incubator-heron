licenses(["notice"])

package(default_visibility = ["//visibility:public"])

pkg_name = "gflags"
pkg_version = "2.2.1"

package_file = pkg_name + "-" + pkg_version + ".tar.gz"
package_dir = pkg_name + "-" + pkg_version

include_files = [
    "include/gflags/gflags.h",
    "include/gflags/gflags_completions.h",
    "include/gflags/gflags_declare.h",
    "include/gflags/gflags_gflags.h",
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
        'cmake -DCMAKE_INSTALL_PREFIX:PATH=$$INSTALL_DIR .',
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
