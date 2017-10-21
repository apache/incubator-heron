licenses(["notice"])

package(default_visibility = ["//visibility:public"])

pkg_name = "glog"
pkg_version = "0.3.5"

package_file = pkg_name + "-" + pkg_version + ".tar.gz"
package_dir = pkg_name + "-" + pkg_version

include_files = [
    "include/glog/log_severity.h",
    "include/glog/logging.h",
    "include/glog/raw_logging.h",
    "include/glog/stl_logging.h",
    "include/glog/vlog_is_on.h",
]

lib_static_files = ["lib/libglog.a"]

lib_mac_dynamic_files = ["lib/libglog.0.dylib"]

lib_linux_dynamic_files = ["lib/libglog.so"]

common_script = [
    'export UNWIND_DIR=$$(pwd)/$(GENDIR)/third_party/libunwind',
    'export INSTALL_DIR=$$(pwd)/$(@D)',
    'export TMP_DIR=$$(mktemp -d -t glog.XXXXX)',
    'mkdir -p $$TMP_DIR',
    'cp -R $(SRCS) $$TMP_DIR',
    'cd $$TMP_DIR',
    'tar xfz ' + package_file,
    'cd ' + package_dir,
]

mac_script = "\n".join(common_script + [
    './configure --prefix=$$INSTALL_DIR',
    'make install',
    'touch $$INSTALL_DIR/lib/libglog.so',
    'rm -rf $$TMP_DIR',
])

linux_script = "\n".join(common_script + [
     'export VAR_LIBS="-Wl,--rpath -Wl,$$UNWIND_DIR/lib -L$$UNWIND_DIR/lib"',
     'export VAR_INCL="-I$$UNWIND_DIR/include"',
     'export VAR_LD="-L$$UNWIND_DIR/lib"',
     './configure --prefix=$$INSTALL_DIR LIBS="$$VAR_LIBS" CPPFLAGS="$$VAR_INCL" LDFLAGS="$$VAR_LD"',
     'make install LIBS="$$VAR_LIBS" CPPFLAGS="$$VAR_INCL" LDFLAGS="$$VAR_LD"',
     'touch $$INSTALL_DIR/lib/libglog.0.dylib',
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
    outs = include_files + lib_static_files + lib_mac_dynamic_files + lib_linux_dynamic_files,
    cmd = select({
        "//tools/platform:darwin": mac_script,
        "//conditions:default": linux_script,
    }),
)

cc_library(
    name = "glog-cxx",
    srcs = ["empty.cc"] + lib_static_files,
    deps = [
      "//third_party/gflags:gflags-cxx",
    ],
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

cc_library(
    name = "glog-shared-cxx",
    srcs = ["empty.cc"] + select({
        "//tools/platform:darwin": lib_mac_dynamic_files,
        "//conditions:default": lib_linux_dynamic_files,
    }),
    deps = [
        "//third_party/gflags:gflags-cxx",
    ],
    hdrs = include_files,
    includes = [
        "include",
    ],
)


filegroup(
    name = "glog",
    srcs = [
        ":glog-cxx",
    ]
)

filegroup(
    name = "glog-files",
    srcs = include_files + select({
        "//tools/platform:darwin": lib_static_files + lib_mac_dynamic_files,
        "//conditions:default": lib_static_files + lib_linux_dynamic_files,
    }),
)

filegroup(
    name = "glog-dynamic-lib-files",
    srcs = select({
        "//tools/platform:darwin": lib_mac_dynamic_files,
        "//conditions:default": lib_linux_dynamic_files
    }),
)
