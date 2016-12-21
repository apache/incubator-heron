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
#ifndef _HDFS_LIBHDFS3_CLIENT_LEASE_RENEW_H_
#define _HDFS_LIBHDFS3_CLIENT_LEASE_RENEW_H_

#include <map>

#include "Atomic.h"
#include "Memory.h"
#include "Thread.h"

namespace Hdfs {
namespace Internal {

class FileSystemInter;

class LeaseRenewer {
public:
    virtual ~LeaseRenewer() {
    }

    virtual void StartRenew(shared_ptr<FileSystemInter> filesystem) = 0;
    virtual void StopRenew(shared_ptr<FileSystemInter> filesystem) = 0;

public:
    static LeaseRenewer & GetLeaseRenewer();
    static void CreateSinglten();

private:
    static once_flag once;
    static shared_ptr<LeaseRenewer> renewer;
};

class LeaseRenewerImpl: public LeaseRenewer {
public:
    LeaseRenewerImpl();
    ~LeaseRenewerImpl();
    int getInterval() const;
    void setInterval(int interval);
    void StartRenew(shared_ptr<FileSystemInter> filesystem);
    void StopRenew(shared_ptr<FileSystemInter> filesystem);

private:
    void renewer();

private:
    atomic<bool> stop;
    condition_variable cond;
    int interval;
    mutex mut;
    std::map<std::string, shared_ptr<FileSystemInter> > maps;
    thread worker;
};

}
}
#endif /* _HDFS_LIBHDFS3_CLIENT_LEASE_RENEW_H_ */
