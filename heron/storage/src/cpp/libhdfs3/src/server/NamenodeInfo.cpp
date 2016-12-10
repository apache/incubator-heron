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
#include "NamenodeInfo.h"
#include "StringUtil.h"
#include "XmlConfig.h"

#include <string>
#include <vector>

using namespace Hdfs::Internal;

namespace Hdfs {

NamenodeInfo::NamenodeInfo() {
}

const char * DFS_NAMESERVICES = "dfs.nameservices";
const char * DFS_NAMENODE_HA = "dfs.ha.namenodes";
const char * DFS_NAMENODE_RPC_ADDRESS_KEY = "dfs.namenode.rpc-address";
const char * DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address";

std::vector<NamenodeInfo> NamenodeInfo::GetHANamenodeInfo(
    const std::string & service, const Config & conf) {
    std::vector<NamenodeInfo> retval;
    std::string strNameNodes = StringTrim(
                                   conf.getString(std::string(DFS_NAMENODE_HA) + "." + service));
    std::vector<std::string> nns = StringSplit(strNameNodes, ",");
    retval.resize(nns.size());

    for (size_t i = 0; i < nns.size(); ++i) {
        std::string dfsRpcAddress = StringTrim(
                                        std::string(DFS_NAMENODE_RPC_ADDRESS_KEY) + "." + service + "."
                                        + StringTrim(nns[i]));
        std::string dfsHttpAddress = StringTrim(
                                         std::string(DFS_NAMENODE_HTTP_ADDRESS_KEY) + "." + service + "."
                                         + StringTrim(nns[i]));
        retval[i].setRpcAddr(StringTrim(conf.getString(dfsRpcAddress, "")));
        retval[i].setHttpAddr(StringTrim(conf.getString(dfsHttpAddress, "")));
    }

    return retval;
}
}
