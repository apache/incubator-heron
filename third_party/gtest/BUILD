licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "gtest"
package_version = "1.7.0"

package_file = package_name + "-" + package_version + ".zip"
package_dir = package_name + "-" + package_version

include_files = [
    "include/gtest/gtest-death-test.h",
    "include/gtest/gtest-param-test.h.pump",
    "include/gtest/gtest-test-part.h",
    "include/gtest/gtest_pred_impl.h",
    "include/gtest/gtest-message.h",
    "include/gtest/gtest-printers.h",
    "include/gtest/gtest-typed-test.h",
    "include/gtest/gtest_prod.h",
    "include/gtest/gtest-param-test.h",
    "include/gtest/gtest-spi.h",
    "include/gtest/gtest.h",

    "include/gtest/internal/gtest-death-test-internal.h",
    "include/gtest/internal/gtest-param-util-generated.h.pump",
    "include/gtest/internal/gtest-tuple.h.pump",
    "include/gtest/internal/gtest-filepath.h",
    "include/gtest/internal/gtest-param-util.h",
    "include/gtest/internal/gtest-type-util.h",
    "include/gtest/internal/gtest-internal.h",
    "include/gtest/internal/gtest-port.h",
    "include/gtest/internal/gtest-type-util.h.pump",
    "include/gtest/internal/gtest-linked_ptr.h",
    "include/gtest/internal/gtest-string.h",
    "include/gtest/internal/gtest-param-util-generated.h",
    "include/gtest/internal/gtest-tuple.h",
]

lib_files = [
    "lib/libgtest.a", 
    "lib/libgtest_main.a",
]

genrule(
    name = "gtest-srcs",
    srcs = [
        package_file, 
    ],
    outs = include_files + lib_files,
    cmd = "\n".join([
        'export TMP_DIR=$$(mktemp -d -t gtest.XXXXX)',
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'unzip -q -o ' + package_file,
        'cd ' + package_dir,
        './configure --prefix=$$INSTALL_DIR --enable-shared=no',
        'make',
        'cp lib/.libs/*.a lib/',
        'cd $$TMP_DIR', 
        'rm -rf $$INSTALL_DIR/*',
        'cp -f -r ' + package_dir + '/*' + ' ' + '$$INSTALL_DIR',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "gtest-cxx",
    srcs = [
        "empty.cc",
        "lib/libgtest.a",
    ],
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

cc_library(
    name = "gtest_main-cxx",
    srcs = [
        "empty.cc",
        "lib/libgtest_main.a",
    ],
    deps = [
        ":gtest-cxx",
    ],
    linkstatic = 1,
    alwayslink = 1,
)

filegroup(
    name = "gtest",
    srcs = [
        ":gtest-cxx",
        ":gtest_main-cxx",
    ]
)

filegroup(
    name = "gtest-files",
    srcs = include_files + lib_files,
)
