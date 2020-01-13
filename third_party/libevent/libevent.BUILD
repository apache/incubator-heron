licenses(["notice"])

package(default_visibility = ["//visibility:public"])

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
    "include/event2/visibility.h",
]

lib_files = [
    "lib/libevent.a", 
    "lib/libevent_core.a",
    "lib/libevent_extra.a", 
    "lib/libevent_pthreads.a",
]

genrule(
    name = "libevent-srcs",
    outs = include_files + lib_files,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t libevent.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $$(pwd)/external/org_libevent_libevent/* $$TMP_DIR',
        'cd $$TMP_DIR',
        'autoreconf -f -i',
        './configure --prefix=$$INSTALL_DIR --enable-shared=no --disable-openssl',
        'make install',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "libevent",
    srcs = ["lib/libevent.a"],
    hdrs = include_files,
    includes = ["include"],  
    linkstatic = 1,
)

filegroup(
    name = "libevent-files",
    srcs = include_files + lib_files,
)
