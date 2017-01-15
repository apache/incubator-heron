licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "jansson"
package_version = "2.5"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

genrule(
    name = "jansson-srcs",
    srcs = [
        package_file, 
    ],
    outs = [
        "include/jansson.h",
        "include/jansson_config.h",
        "lib/libjansson.a",
    ],
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t jansson.XXXXX)",
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
    name = "jansson-cxx",
    srcs = [
        "include/jansson_config.h",

        "empty.cc",
        "lib/libjansson.a",
    ],
    hdrs = [
        "include/jansson.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "jansson",
    srcs = [
        ":jansson-cxx",
    ]
)
