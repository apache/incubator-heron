licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "glog"
package_version = "0.3.4"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

COMMON_EXECUTE = [
    "export UNWIND_DIR=$$(pwd)/$(GENDIR)/third_party/libunwind",
    "export INSTALL_DIR=$$(pwd)/$(@D)",
    "export TMP_DIR=$$(mktemp -d -t glog.XXXXX)",
    "mkdir -p $$TMP_DIR",
    "cp -R $(SRCS) $$TMP_DIR",
    "cd $$TMP_DIR",
    "tar xfz " + package_file,
    "cd " + package_dir,
]

DARWIN_EXECUTE = "\n".join(COMMON_EXECUTE + [
    "./configure --prefix=$$INSTALL_DIR --enable-shared=no",
    "make install",
    "rm -rf $$TMP_DIR",
])

LINUX_EXECUTE = "\n".join(COMMON_EXECUTE + [
     './configure --prefix=$$INSTALL_DIR --enable-shared=no LIBS="-Wl,--rpath -Wl,$$UNWIND_DIR/lib -L$$UNWIND_DIR/lib" CPPFLAGS=-I$$UNWIND_DIR/include LDFLAGS=-L$$UNWIND_DIR/lib',
     'make install LIBS="-Wl,--rpath -Wl,$$UNWIND_DIR/lib -L$$UNWIND_DIR/lib" CPPFLAGS=-I$$UNWIND_DIR/include LDFLAGS=-L$$UNWIND_DIR/lib',
     'rm -rf $$TMP_DIR',
])

genrule(
    name = "glog-srcs",
    srcs = select({
        "//tools/platform:darwin": [package_file],
        "//conditions:default": [
            package_file,
            "//third_party/libunwind:libunwind-files",
         ]
    }),
    outs = [
        "include/glog/log_severity.h",
        "include/glog/logging.h",
        "include/glog/raw_logging.h",
        "include/glog/stl_logging.h",
        "include/glog/vlog_is_on.h",
        "lib/libglog.a",
    ],
    cmd = select({
        "//tools/platform:darwin": DARWIN_EXECUTE,
        "//conditions:default": LINUX_EXECUTE,
    }),
)

cc_library(
    name = "glog-cxx",
    srcs = [
        "empty.cc",
        "lib/libglog.a",
        "include/glog/log_severity.h",
        "include/glog/raw_logging.h",
        "include/glog/stl_logging.h",
        "include/glog/vlog_is_on.h",
    ],
    deps = [
      "//third_party/gflags:gflags-cxx",
    ],
    hdrs = [
        "include/glog/logging.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "glog",
    srcs = [
        ":glog-cxx",
    ]
)
