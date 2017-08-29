licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "googletest-release"
package_version = "1.8.0"

package_file = package_name + "-" + package_version + ".zip"
package_dir = package_name + "-" + package_version

gtest_include_files = [
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

    "include/gtest/internal/custom/gtest-port.h",
    "include/gtest/internal/custom/gtest-printers.h",
    "include/gtest/internal/custom/gtest.h",

    "include/gtest/internal/gtest-death-test-internal.h",
    "include/gtest/internal/gtest-param-util-generated.h.pump",
    "include/gtest/internal/gtest-tuple.h.pump",
    "include/gtest/internal/gtest-filepath.h",
    "include/gtest/internal/gtest-param-util.h",
    "include/gtest/internal/gtest-type-util.h",
    "include/gtest/internal/gtest-internal.h",
    "include/gtest/internal/gtest-port.h",
    "include/gtest/internal/gtest-port-arch.h",
    "include/gtest/internal/gtest-type-util.h.pump",
    "include/gtest/internal/gtest-linked_ptr.h",
    "include/gtest/internal/gtest-string.h",
    "include/gtest/internal/gtest-param-util-generated.h",
    "include/gtest/internal/gtest-tuple.h",
]

gtest_lib_files = [
    "lib/libgtest.a", 
    "lib/libgtest_main.a",
]

gmock_include_files = [
    "include/gmock/gmock-actions.h",
    "include/gmock/gmock-generated-function-mockers.h.pump",
    "include/gmock/gmock-matchers.h",
    "include/gmock/gmock-cardinalities.h",
    "include/gmock/gmock-generated-matchers.h",
    "include/gmock/gmock-more-actions.h",
    "include/gmock/gmock-generated-actions.h",
    "include/gmock/gmock-generated-matchers.h.pump",
    "include/gmock/gmock-more-matchers.h",
    "include/gmock/gmock-generated-actions.h.pump",
    "include/gmock/gmock-generated-nice-strict.h",
    "include/gmock/gmock-spec-builders.h",
    "include/gmock/gmock-generated-function-mockers.h",
    "include/gmock/gmock-generated-nice-strict.h.pump",
    "include/gmock/gmock.h",

    "include/gmock/internal/custom/gmock-generated-actions.h",
    "include/gmock/internal/custom/gmock-generated-actions.h.pump",
    "include/gmock/internal/custom/gmock-matchers.h",
    "include/gmock/internal/custom/gmock-port.h",

    "include/gmock/internal/gmock-generated-internal-utils.h",
    "include/gmock/internal/gmock-internal-utils.h",
    "include/gmock/internal/gmock-generated-internal-utils.h.pump",
    "include/gmock/internal/gmock-port.h",
]

gmock_lib_files = [
    "lib/libgmock.a",
    "lib/libgmock_main.a",
]

include_files = gtest_include_files + gmock_include_files
lib_files = gtest_lib_files + gmock_lib_files

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
        'cmake -DCMAKE_INSTALL_PREFIX:PATH=$$INSTALL_DIR .',
        'make install',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "gtest-cxx",
    srcs = [
        "empty.cc",
        "lib/libgtest.a",
    ],
    hdrs = gtest_include_files,
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
    srcs = gtest_include_files + gtest_lib_files,
)

cc_library(
    name = "gmock-cxx",
    srcs = [
        "empty.cc",
        "lib/libgmock.a",
    ],
    hdrs = gmock_include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

cc_library(
    name = "gmock_main-cxx",
    srcs = [
        "empty.cc",
        "lib/libgmock_main.a",
    ],
    linkstatic = 1,
)

filegroup(
    name = "gmock",
    srcs = [
        ":gmock-cxx",
        ":gmock_main-cxx",
    ]
)

filegroup(
    name = "gmock-files",
    srcs = gmock_include_files + gmock_lib_files,
)
