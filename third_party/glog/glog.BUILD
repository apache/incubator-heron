licenses(["notice"])

package(default_visibility = ["//visibility:public"])

config_setting(
    name = "darwin",
    values = {
        "cpu": "darwin",
    },
    visibility = ["//visibility:public"],
)

config_setting(
    name = "k8",
    values = {
        "cpu": "k8",
    },
    visibility = ["//visibility:public"],
)

include_files = [
    "include/glog/log_severity.h",
    "include/glog/logging.h",
    "include/glog/raw_logging.h",
    "include/glog/stl_logging.h",
    "include/glog/vlog_is_on.h",
]

lib_files = [
    "lib/libglog.a",
]

common_script = [
    'export UNWIND_DIR=$$(pwd)/$(GENDIR)/external/org_nongnu_libunwind',
    'echo $$UNWIND_DIR',
    'export INSTALL_DIR=$$(pwd)/$(@D)',
    'export TMP_DIR=$$(mktemp -d -t glog.XXXXX)',
    'mkdir -p $$TMP_DIR',
    'cp -R $$(pwd)/external/com_github_google_glog/* $$TMP_DIR', 
    'cd $$TMP_DIR',
]

mac_script = "\n".join(common_script + [
    './configure --prefix=$$INSTALL_DIR --enable-shared=no',
    'make install',
    'rm -rf $$TMP_DIR',
])

linux_script = "\n".join(common_script + [
     'export VAR_LIBS="-Wl,--rpath -Wl,$$UNWIND_DIR/lib -L$$UNWIND_DIR/lib"',
     'export VAR_INCL="-I$$UNWIND_DIR/include"',
     'export VAR_LD="-L$$UNWIND_DIR/lib"',
     'autoreconf -f -i',
     './configure --prefix=$$INSTALL_DIR --enable-shared=no LIBS="$$VAR_LIBS" CPPFLAGS="$$VAR_INCL" LDFLAGS="$$VAR_LD"',
     'make install LIBS="$$VAR_LIBS" CPPFLAGS="$$VAR_INCL" LDFLAGS="$$VAR_LD"',
     'rm -rf $$TMP_DIR',
])

genrule(
    name = "glog-srcs",
    srcs = select({
        ":darwin": [],
        "//conditions:default": ["@org_apache_heron//third_party/libunwind:libunwind-files"]
    }),
    outs = include_files + lib_files,
    cmd = select({
        ":darwin": mac_script,
        "//conditions:default": linux_script,
    }),
)

cc_library(
    name = "glog",
    srcs = lib_files,
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "glog-files",
    srcs = include_files + lib_files
)
