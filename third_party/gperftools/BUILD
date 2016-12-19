licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "gperftools"
package_version = "2.4"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

COMMON_EXECUTE = [
    "export UNWIND_DIR=$$(pwd)/$(GENDIR)/third_party/libunwind",
    "export INSTALL_DIR=$$(pwd)/$(@D)",
    "export TMP_DIR=$$(mktemp -d -t gperftools.XXXXX)",
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
     './configure --prefix=$$INSTALL_DIR --enable-shared=no CPPFLAGS=-I$$UNWIND_DIR/include LDFLAGS="-L$$UNWIND_DIR/lib -lunwind" --enable-frame-pointers',
     'make install CPPFLAGS=-I$$UNWIND_DIR/include LDFLAGS="-L$$UNWIND_DIR/lib -lunwind"',
     'rm -rf $$TMP_DIR',
])

genrule(
    name = "gperftools-srcs",
    srcs = select({
        "//tools/platform:darwin": [package_file],
        "//conditions:default": [
            package_file,
            "//third_party/libunwind:libunwind-files",
         ]
    }),
    outs = [
        "bin/pprof",

        "include/google/heap-checker.h",
        "include/google/heap-profiler.h",
        "include/google/malloc_extension.h",
        "include/google/malloc_extension_c.h",
        "include/google/malloc_hook.h",
        "include/google/malloc_hook_c.h",
        "include/google/profiler.h",
        "include/google/stacktrace.h",
        "include/google/tcmalloc.h",

        "include/gperftools/heap-checker.h",
        "include/gperftools/heap-profiler.h",
        "include/gperftools/malloc_extension.h",
        "include/gperftools/malloc_extension_c.h",
        "include/gperftools/malloc_hook.h",
        "include/gperftools/malloc_hook_c.h",
        "include/gperftools/profiler.h",
        "include/gperftools/stacktrace.h",
        "include/gperftools/tcmalloc.h",

        "lib/libprofiler.a",
        "lib/libtcmalloc.a",
        "lib/libtcmalloc_and_profiler.a",
        "lib/libtcmalloc_debug.a",
        "lib/libtcmalloc_minimal.a",
        "lib/libtcmalloc_minimal_debug.a",
    ],
    cmd = select({
        "//tools/platform:darwin": DARWIN_EXECUTE,
        "//conditions:default": LINUX_EXECUTE,
    }),
)

cc_library(
    name = "tcmalloc-cxx",
    srcs = [
        "empty.cc",
        "lib/libtcmalloc.a",
    ],
    hdrs = [
        "include/gperftools/tcmalloc.h",
        "include/gperftools/malloc_extension.h",
        "include/google/malloc_extension.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)

cc_library(
    name = "profiler-cxx",
    srcs = [
        "empty.cc",
        "lib/libprofiler.a",
    ],
    hdrs = [
        "include/gperftools/profiler.h",
    ],
    includes = ["include"],
    linkstatic = 1,
)

filegroup(
    name = "gperftools",
    srcs = [
        "bin/pprof",
        ":profiler-cxx",
        ":tcmalloc-cxx",
    ],
)
