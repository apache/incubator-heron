licenses(["notice"])

package(default_visibility = ["//visibility:public"])

include_files = [
    "include/openssl/err.h",
    "include/openssl/opensslconf.h",
    "include/openssl/ssl.h",
]

lib_files = [
    "lib/libssl.so",
    "lib/libcrypto.so",
]

genrule(
    name = "openssl-srcs",
    outs = include_files + lib_files,
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t openssl.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -r --dereference $$(pwd)/external/org_openssl_openssl/* $$TMP_DIR",
        "cd $$TMP_DIR",
        "./config --prefix=$$INSTALL_DIR",
        "make install",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "openssl",
    srcs = lib_files,
    hdrs = include_files,
    includes = [ "include", ],
)

filegroup(
    name = "openssl-files",
    srcs = include_files + lib_files,
)
