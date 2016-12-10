package(default_visibility = ["//visibility:public"])

load("/tools/rules/genproto",  "proto_library")
load("/tools/rules/pex_rules", "pex_library")

proto_library(
    name = "proto_security",
    src = "Security.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
)

proto_library(
    name = "proto_hdfs",
    src = "hdfs.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
    deps = [":proto_security"],
)

proto_library(
    name = "proto_ipc_connection_context",
    src = "IpcConnectionContext.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
)

proto_library(
    name = "proto_rpc_header",
    src = "RpcHeader.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
)

proto_library(
    name = "proto_protobuf_rpc_engine",
    src = "ProtobufRpcEngine.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
)

proto_library(
    name = "proto_datatransfer",
    src = "datatransfer.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
    deps = [
        ":proto_security",
        ":proto_hdfs",
    ],
)

proto_library(
    name = "proto_client_datanode_protocol",
    src = "ClientDatanodeProtocol.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
    deps = [
        ":proto_security",
        ":proto_hdfs",
    ],
)

proto_library(
    name = "proto_client_namenode_protocol",
    src = "ClientNamenodeProtocol.proto",
    includes = ["heron/storage/src/cpp/libhdfs3/proto"],
    gen_cc = 1,
    deps = [
        ":proto_security",
        ":proto_hdfs",
    ],
)

cc_library(
    name = "proto-cxx",
    srcs = [
        "empty.cc",
    ],
    hdrs = [
        "Security.pb.h",
        "hdfs.pb.h",
        "IpcConnectionContext.pb.h",
        "RpcHeader.pb.h",
        "ProtobufRpcEngine.pb.h",
        "datatransfer.pb.h",    
        "ClientDatanodeProtocol.pb.h",
        "ClientNamenodeProtocol.pb.h",
    ],
    deps = [
        ":proto_security_cc",
        ":proto_hdfs_cc",
        ":proto_ipc_connection_context_cc",
        ":proto_rpc_header_cc",
        ":proto_protobuf_rpc_engine_cc",
        ":proto_client_datanode_protocol_cc",
        ":proto_client_namenode_protocol_cc",
        ":proto_datatransfer_cc",
        "//third_party/protobuf:protobuf-cxx",
    ],
    linkstatic = 1,
)
