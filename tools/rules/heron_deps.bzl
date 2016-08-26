################################################################################
# Convenience macro for Heron proto files
################################################################################
def heron_java_proto_files():
    return [
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_packing_plan_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/proto:proto_topology_java",
        "//heron/proto:proto_tuple_java",
        "//heron/proto:proto_stmgr_java",
        "@com_google_protobuf_protobuf_java//jar",
    ]
