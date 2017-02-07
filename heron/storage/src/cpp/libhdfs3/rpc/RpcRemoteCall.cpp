/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "rpc/RpcRemoteCall.h"

#include <google/protobuf/io/coded_stream.h>
#include <string>
#include <vector>

#include "common/Memory.h"
#include "proto/ProtobufRpcEngine.pb.h"
#include "rpc/RpcCall.h"
#include "rpc/RpcContentWrapper.h"
#include "proto/RpcHeader.pb.h"
#include "common/WriteBuffer.h"

#define PING_CALL_ID -4

// using namespace google::protobuf::io;

namespace Hdfs {
namespace Internal {

void RpcRemoteCall::serialize(const RpcProtocolInfo & protocol,
                              WriteBuffer & buffer) {
    RpcRequestHeaderProto rpcHeader;
    rpcHeader.set_callid(identity);
    rpcHeader.set_clientid(clientId);
    rpcHeader.set_retrycount(-1);
    rpcHeader.set_rpckind(RPC_PROTOCOL_BUFFER);
    rpcHeader.set_rpcop(RpcRequestHeaderProto_OperationProto_RPC_FINAL_PACKET);
    RequestHeaderProto requestHeader;
    requestHeader.set_methodname(call.getName());
    requestHeader.set_declaringclassprotocolname(protocol.getProtocol());
    requestHeader.set_clientprotocolversion(protocol.getVersion());
    RpcContentWrapper wrapper(&requestHeader, call.getRequest());
    int rpcHeaderLen = rpcHeader.ByteSize();
    int size = ::google::protobuf::io::CodedOutputStream::VarintSize32(rpcHeaderLen) + rpcHeaderLen + wrapper.getLength();  // NOLINT(whitespace/line_length)
    buffer.writeBigEndian(size);
    buffer.writeVarint32(rpcHeaderLen);
    rpcHeader.SerializeToArray(buffer.alloc(rpcHeaderLen), rpcHeaderLen);
    wrapper.writeTo(buffer);
}

std::vector<char> RpcRemoteCall::GetPingRequest(const std::string & clientid) {
    WriteBuffer buffer;
    std::vector<char> retval;
    RpcRequestHeaderProto pingHeader;
    pingHeader.set_callid(PING_CALL_ID);
    pingHeader.set_clientid(clientid);
    pingHeader.set_retrycount(INVALID_RETRY_COUNT);
    pingHeader.set_rpckind(RpcKindProto::RPC_PROTOCOL_BUFFER);
    pingHeader.set_rpcop(RpcRequestHeaderProto_OperationProto_RPC_FINAL_PACKET);
    int rpcHeaderLen = pingHeader.ByteSize();
    int size = ::google::protobuf::io::CodedOutputStream::VarintSize32(rpcHeaderLen) + rpcHeaderLen;
    buffer.writeBigEndian(size);
    buffer.writeVarint32(rpcHeaderLen);
    pingHeader.SerializeWithCachedSizesToArray(reinterpret_cast<unsigned char *>(buffer.alloc(pingHeader.ByteSize())));  // NOLINT(whitespace/line_length)
    retval.resize(buffer.getDataSize(0));
    memcpy(&retval[0], buffer.getBuffer(0), retval.size());
    return retval;
}

}  // namespace Internal
}  // namespace Hdfs

