licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "gsasl"
package_version = "1.8.0"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

genrule(
    name = "gsasl-srcs",
    srcs = [
        package_file,
    ],
    outs = [
        "include/gsasl-compat.h",
        "include/gsasl-mech.h",
        "include/gsasl.h",
        "lib/libgsasl.a",
    ],
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
    srcs = [
        "empty.cc",
        "include/gsasl-compat.h",
        "include/gsasl-mech.h",
        "lib/libgsasl.a",
    ],
    hdrs = [
        "include/gsasl.h",
    ],
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
