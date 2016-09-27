package(default_visibility = ["//visibility:public"])

load("/tools/rules/genproto", "proto_library")

cc_library(
    name = "network-cxx",
    srcs = [
        "asyncdns.cpp",
        "baseclient.cpp",
        "baseconnection.cpp",
        "baseserver.cpp",
        "client.cpp",
        "connection.cpp",
        "event_loop_impl.cpp",
        "httpclient.cpp",
        "httpserver.cpp",
        "httputils.cpp",
        "modinit.cpp",
        "networkoptions.cpp",
        "packet.cpp",
        "server.cpp",

        "regevent.h",
        "asyncdns.h",
        "baseclient.h",
        "baseconnection.h",
        "baseserver.h",
        "client.h",
        "connection.h",
        "event_loop.h",
        "httpclient.h",
        "httpserver.h",
        "httputils.h",
        "networkoptions.h",
        "network_error.h",
        "packet.h",
        "event_loop_impl.h",
        "server.h",
        "mempool.h",
    ],
    hdrs = [
        "network.h",
        "modinit.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        "//third_party/libevent:libevent-cxx",
        "//heron/common/src/cpp/threads:threads-cxx",
        "//heron/common/src/cpp/errors:errors-cxx",
        "//heron/common/src/cpp/basics:basics-cxx",
    ],
    linkstatic = 1,
)
