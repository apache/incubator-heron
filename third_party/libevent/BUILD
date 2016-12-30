licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "libevent"
package_version = "2.0.21-stable"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [
    "include/evdns.h",
    "include/event.h",
    "include/evhttp.h",
    "include/evrpc.h",
    "include/evutil.h",

    "include/event2/buffer.h",
    "include/event2/bufferevent_struct.h",
    "include/event2/event.h",
    "include/event2/http_struct.h",
    "include/event2/rpc_struct.h",
    "include/event2/buffer_compat.h",      
    "include/event2/dns.h",
    "include/event2/event_compat.h",
    "include/event2/keyvalq_struct.h", 
    "include/event2/tag.h",
    "include/event2/bufferevent.h",
    "include/event2/dns_compat.h",
    "include/event2/event_struct.h",
    "include/event2/listener.h",
    "include/event2/tag_compat.h",
    "include/event2/bufferevent_compat.h",
    "include/event2/dns_struct.h",
    "include/event2/http.h",
    "include/event2/rpc.h",
    "include/event2/thread.h",
    "include/event2/event-config.h",
    "include/event2/http_compat.h",
    "include/event2/rpc_compat.h",
    "include/event2/util.h",
]

lib_files = [
    "lib/libevent.a", 
    "lib/libevent_core.a",
    "lib/libevent_extra.a", 
    "lib/libevent_pthreads.a",
]

genrule(
    name = "libevent-srcs",
    srcs = [
        package_file, 
    ],
    outs = include_files + lib_files,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t libevent.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'cd ' + package_dir,
        './configure --prefix=$$INSTALL_DIR --enable-shared=no --disable-openssl',
        'make install',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "libevent-cxx",
    srcs = [ 
        "empty.cc",
        "lib/libevent.a",
    ],
    hdrs = include_files,
    includes = [ 
        "include",
    ],  
    linkstatic = 1,
)

filegroup(
    name = "libevent-files",
    srcs = include_files + lib_files,
)
