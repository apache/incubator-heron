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
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemInter.h"
#include "LeaseRenewer.h"
#include "Logger.h"

#define DEFAULT_LEASE_RENEW_INTERVAL (60 * 1000)

namespace Hdfs {
namespace Internal {

once_flag LeaseRenewer::once;
shared_ptr<LeaseRenewer> LeaseRenewer::renewer;

LeaseRenewer & LeaseRenewer::GetLeaseRenewer() {
    call_once(once, &LeaseRenewer::CreateSinglten);
    assert(renewer);
    return *renewer;
}

void LeaseRenewer::CreateSinglten() {
    renewer = shared_ptr < LeaseRenewer > (new LeaseRenewerImpl());
}

LeaseRenewerImpl::LeaseRenewerImpl() :
    stop(true), interval(DEFAULT_LEASE_RENEW_INTERVAL) {
}

LeaseRenewerImpl::~LeaseRenewerImpl() {
    stop = true;
    cond.notify_all();

    if (worker.joinable()) {
        worker.join();
    }
}

int LeaseRenewerImpl::getInterval() const {
    return interval;
}

void LeaseRenewerImpl::setInterval(int interval) {
    this->interval = interval;
}

void LeaseRenewerImpl::StartRenew(shared_ptr<FileSystemInter> filesystem) {
    lock_guard<mutex> lock(mut);
    const char * clientName = filesystem->getClientName();

    if (maps.find(clientName) == maps.end()) {
        maps[clientName] = filesystem;
    }

    filesystem->registerOpenedOutputStream();

    if (stop && !maps.empty()) {
        if (worker.joinable()) {
            worker.join();
        }

        stop = false;
        CREATE_THREAD(worker, bind(&LeaseRenewerImpl::renewer, this));
    }
}

void LeaseRenewerImpl::StopRenew(shared_ptr<FileSystemInter> filesystem) {
    lock_guard<mutex> lock(mut);
    const char * clientName = filesystem->getClientName();

    if (filesystem->unregisterOpenedOutputStream()
            && maps.find(clientName) != maps.end()) {
        maps.erase(clientName);
    }
}

void LeaseRenewerImpl::renewer() {
    assert(stop == false);

    while (!stop) {
        try {
            unique_lock < mutex > lock(mut);
            cond.wait_for(lock, milliseconds(interval));

            if (stop || maps.empty()) {
                break;
            }

            std::map<std::string, shared_ptr<FileSystemInter> >::iterator s, e, d;
            e = maps.end();

            for (s = maps.begin(); s != e;) {
                shared_ptr<FileSystemInter> fs = s->second;

                try {
                    if (!fs->renewLease()) {
                        d = s++;
                        maps.erase(d);
                    } else {
                        ++s;
                    }

                    continue;
                } catch (const HdfsException & e) {
                    std::string buffer;
                    LOG(LOG_ERROR,
                        "Failed to renew lease for filesystem which client name is %s, since:\n%s",
                        fs->getClientName(), GetExceptionDetail(e, buffer));
                } catch (const std::exception & e) {
                    LOG(LOG_ERROR,
                        "Failed to renew lease for filesystem which client name is %s, since:\n%s",
                        fs->getClientName(), e.what());
                    break;
                }

                ++s;
            }

            if (maps.empty()) {
                break;
            }
        } catch (const std::bad_alloc & e) {
            /*
             * keep quiet if we run out of memory, since writing log needs memory,
             * that may cause the process terminated.
             */
            break;
        } catch (const std::exception & e) {
            LOG(LOG_ERROR,
                "Lease renewer will exit since unexpected exception: %s",
                e.what());
            break;
        }
    }

    stop = true;
}

}
}
