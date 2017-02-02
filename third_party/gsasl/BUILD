licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "gsasl"
package_version = "1.8.0"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [
    "include/gsasl-compat.h",
    "include/gsasl-mech.h",
    "include/gsasl.h",
]

lib_files = [
    "lib/libgsasl.a",
]

genrule(
    name = "gsasl-srcs",
    srcs = [
        package_file,
    ],
    outs = include_files + lib_files,
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t gsasl.XXXXX)",
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
    name = "gsasl-cxx",
    srcs = ["empty.cc"] + lib_files,
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "gsasl",
    srcs = [
        ":gsasl-cxx",
    ]
)

filegroup(
    name = "gsasl-files",
    srcs = include_files + lib_files,
)
