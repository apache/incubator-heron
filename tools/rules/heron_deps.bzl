#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

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
        "//heron/proto:proto_ckptmgr_java",
        "//heron/proto:proto_tmanager_java",
        "//heron/proto:proto_topology_java",
        "//heron/proto:proto_tuple_java",
        "//heron/proto:proto_stmgr_java",
        "@maven//:com_google_protobuf_protobuf_java",
    ]

def heron_java_api_proto_files():
    return [
        "//heron/proto:proto_topology_java",
        "@maven//:com_google_protobuf_protobuf_java",
    ]
