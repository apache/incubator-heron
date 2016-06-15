licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "glog"
package_version = "0.3.4"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

genrule(
    name = "glog-srcs",
    srcs = [
        package_file,
        "//3rdparty/libunwind:libunwind-files",
    ],
    outs = [
        "include/glog/log_severity.h",
        "include/glog/logging.h",
        "include/glog/raw_logging.h",
        "include/glog/stl_logging.h",
        "include/glog/vlog_is_on.h",
        "lib/libglog.a",
    ],
    cmd = "\n".join([
        "export UNWIND_DIR=$$(pwd)/$(GENDIR)/3rdparty/libunwind",
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t glog.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "tar xfz " + package_file,
        "cd " + package_dir,
        './configure --prefix=$$INSTALL_DIR --enable-shared=no LIBS="-Wl,--rpath -Wl,$$UNWIND_DIR/lib -L$$UNWIND_DIR/lib" CPPFLAGS=-I$$UNWIND_DIR/include LDFLAGS=-L$$UNWIND_DIR/lib',
        'make install LIBS="-Wl,--rpath -Wl,$$UNWIND_DIR/lib -L$$UNWIND_DIR/lib" CPPFLAGS=-I$$UNWIND_DIR/include LDFLAGS=-L$$UNWIND_DIR/lib',
        "rm -rf $$TMP_DIR",
    ]),
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
