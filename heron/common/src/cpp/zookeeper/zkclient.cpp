/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

///////////////////////////////////////////////////////////////////////////////
// Implements the ZKClient using the ZKClient library as the underlying
// transport implementation. See zkclient.h for API details.
///////////////////////////////////////////////////////////////////////////////
#include "zookeeper/zkclient.h"
#include <fcntl.h>
#include <errno.h>
#include <string>
#include <vector>
#include "glog/logging.h"

// used for get calls
struct ZKClientGetStructure {
  std::string* result_;
  sp_int32* version_;
  CallBack1<sp_int32>* cb_;
};

// used for get children calls
struct ZKClientGetChildrenStructure {
  std::vector<std::string>* result_;
  CallBack1<sp_int32>* cb_;
};

void RunUserCb(sp_int32 rc, VCallback<sp_int32> cb) { cb(rc); }

void RunWatcherCb(VCallback<> cb) { cb(); }

// A helper method to run watch event callback
void RunWatchEventCb(VCallback<ZKClient::ZkWatchEvent> watch_cb, ZKClient::ZkWatchEvent event) {
  watch_cb(event);
}

// 'C' style callback for zk on global wathcer events
void CallGlobalWatcher(zhandle_t* _zh, sp_int32 _type, sp_int32 _state, const char* _path,
                       void* _context) {
  ZKClient* cl = reinterpret_cast<ZKClient*>(_context);
  cl->GlobalWatcher(_zh, _type, _state, _path);
}

// 'C' style callback for zk watcher
void CallWatcher(zhandle_t*, sp_int32 _type, sp_int32 _state, const char* _path, void* _context) {
  LOG(INFO) << "ZKClient CallWatcher called with type " << ZKClient::type2String(_type)
            << " and state " << ZKClient::state2String(_state);
  if (_path && strlen(_path) > 0) {
    LOG(INFO) << " for path " << _path;
  }

  if (_state == ZOO_CONNECTED_STATE) {
    // Only Handle watches when in connected state
    CallBack* cb = reinterpret_cast<CallBack*>(_context);
    cb->Run();
  }
}

void StringCompletionWatcher(sp_int32 _rc, const char*, const void* _data) {
  CallBack1<sp_int32>* cb = (CallBack1<sp_int32>*)(_data);
  cb->Run(_rc);
}

void VoidCompletionWatcher(sp_int32 _rc, const void* _data) {
  CallBack1<sp_int32>* cb = (CallBack1<sp_int32>*)(_data);
  cb->Run(_rc);
}

void GetCompletionWatcher(int _rc, const char* _value, int _value_len, const struct Stat* _stat,
                          const void* _data) {
  const ZKClientGetStructure* get_structure = reinterpret_cast<const ZKClientGetStructure*>(_data);
  std::string* result = get_structure->result_;
  if (_rc == 0 && _value) {
    result->resize(_value_len);
    result->assign(_value, _value_len);
    if (get_structure->version_) {
      *(get_structure->version_) = _stat->version;
    }
  }
  CallBack1<sp_int32>* cb = get_structure->cb_;
  delete get_structure;
  cb->Run(_rc);
}

void SetCompletionWatcher(sp_int32 _rc, const struct Stat*, const void* _data) {
  CallBack1<sp_int32>* cb = (CallBack1<sp_int32>*)(_data);
  cb->Run(_rc);
}

void GetChildrenCompletionWatcher(sp_int32 _rc, const struct String_vector* _strings,
                                  const void* _data) {
  const ZKClientGetChildrenStructure* get_structure =
      reinterpret_cast<const ZKClientGetChildrenStructure*>(_data);
  std::vector<std::string>* result = get_structure->result_;
  if (_rc == 0 && _strings) {
    for (sp_int32 i = 0; i < _strings->count; i++) {
      result->push_back(_strings->data[i]);
    }
  }
  CallBack1<sp_int32>* cb = get_structure->cb_;
  delete get_structure;
  cb->Run(_rc);
}

void ExistsCompletionHandler(sp_int32 _rc, const struct Stat*, const void* _data) {
  CallBack1<sp_int32>* cb = (CallBack1<sp_int32>*)_data;
  cb->Run(_rc);
}

// Constructor. We create a new event_base.
ZKClient::ZKClient(const std::string& hostportlist, std::shared_ptr<EventLoop> eventLoop)
    : eventLoop_(eventLoop), hostportlist_(hostportlist) {
  Init();
}

ZKClient::ZKClient(const std::string& hostportlist, std::shared_ptr<EventLoop> eventLoop,
                   VCallback<ZkWatchEvent> global_watcher_cb)
    : eventLoop_(eventLoop),
      hostportlist_(hostportlist),
      client_global_watcher_cb_(std::move(global_watcher_cb)) {
  CHECK(client_global_watcher_cb_);
  Init();
}

void ZKClient::Init() {
  piper_ = new Piper(eventLoop_);
  zoo_deterministic_conn_order(0);  // even distribution of clients on the server
  InitZKHandle();
}

// Destructor.
ZKClient::~ZKClient() {
  zookeeper_close(zk_handle_);
  // zookeeper_close() depends on piper_
  // when HeronZKStateMgr::GlobalWatchEventHandler() and GetCompletionWatcher
  // are called at the same time in two threads,
  // thus `delete piper_` after zookeeper_close() joins all zk_client threads.
  delete piper_;
}

//
// Client implementions
//

void ZKClient::Exists(const std::string& _node, VCallback<sp_int32> cb) {
  Exists(_node, VCallback<>(), std::move(cb));
}

void ZKClient::Exists(const std::string& _node, VCallback<> watcher, VCallback<sp_int32> cb) {
  LOG(INFO) << "Checking if " << _node << " exists";
  sp_int32 rc;
  if (!watcher) {
    rc = zoo_aexists(zk_handle_, _node.c_str(), 0, ExistsCompletionHandler,
                     CreateCallback(this, &ZKClient::ZkActionCb, std::move(cb)));
  } else {
    rc = zoo_awexists(zk_handle_, _node.c_str(), CallWatcher,
                      CreateCallback(this, &ZKClient::ZkWatcherCb, std::move(watcher)),
                      ExistsCompletionHandler,
                      CreateCallback(this, &ZKClient::ZkActionCb, std::move(cb)));
  }
  if (rc) {
    // There is nothing we can do here. Continuing will only make
    // other things fail
    LOG(FATAL) << "zoo_aexists/awexists returned non-zero " << rc << " errno: " << errno
               << " while checking for node " << _node << "\n";
  }
}

// Creates a node
void ZKClient::CreateNode(const std::string& _node, const std::string& _value, bool _is_ephimeral,
                          VCallback<sp_int32> cb) {
  sp_int32 flags = 0;
  if (_is_ephimeral) {
    flags |= ZOO_EPHEMERAL;
  }
  LOG(INFO) << "Creating zknode " << _node << std::endl;
  sp_int32 rc = zoo_acreate(zk_handle_, _node.c_str(), _value.c_str(), _value.size(),
                            &ZOO_OPEN_ACL_UNSAFE, flags, StringCompletionWatcher,
                            CreateCallback(this, &ZKClient::ZkActionCb, std::move(cb)));
  if (rc) {
    // There is nothing we can do here. Continuing will only make
    // other things fail
    LOG(FATAL) << "zoo_acreate returned non-zero " << rc << " errno: " << errno
               << " while creating node " << _node << "\n";
  }
}

// Deletes a node
void ZKClient::DeleteNode(const std::string& _node, VCallback<sp_int32> cb) {
  LOG(INFO) << "Deleting zknode " << _node << std::endl;
  sp_int32 rc = zoo_adelete(zk_handle_, _node.c_str(), -1, VoidCompletionWatcher,
                            CreateCallback(this, &ZKClient::ZkActionCb, std::move(cb)));
  if (rc) {
    // There is nothing we can do here. Continuing will only make
    // other things fail
    LOG(FATAL) << "zoo_adelete returned non-zero " << rc << " errno: " << errno
               << " while deleting node " << _node << "\n";
  }
}

void ZKClient::Get(const std::string& _node, std::string* _data, VCallback<sp_int32> cb) {
  Get(_node, _data, NULL, std::move(cb));
}

void ZKClient::Get(const std::string& _node, std::string* _data, sp_int32* _version,
                   VCallback<sp_int32> cb) {
  Get(_node, _data, _version, VCallback<>(), std::move(cb));
}

void ZKClient::Get(const std::string& _node, std::string* _data, sp_int32* _version,
                   VCallback<> watcher, VCallback<sp_int32> cb) {
  LOG(INFO) << "Getting zknode " << _node << std::endl;
  ZKClientGetStructure* get_structure = new ZKClientGetStructure();
  get_structure->result_ = _data;
  get_structure->version_ = _version;
  get_structure->cb_ = CreateCallback(this, &ZKClient::ZkActionCb, std::move(cb));
  sp_int32 rc;
  if (!watcher) {
    rc = zoo_aget(zk_handle_, _node.c_str(), 0, GetCompletionWatcher, get_structure);
  } else {
    rc = zoo_awget(zk_handle_, _node.c_str(), CallWatcher,
                   CreateCallback(this, &ZKClient::ZkWatcherCb, std::move(watcher)),
                   GetCompletionWatcher, get_structure);
  }
  if (rc) {
    // There is nothing we can do here. Continuing will only make
    // other things fail
    LOG(FATAL) << "zoo_aget/zoo_awget returned non-zero " << rc << " errno: " << errno
               << " while getting " << _node << "\n";
  }
}

void ZKClient::Set(const std::string& _node, const std::string& _data, VCallback<sp_int32> cb) {
  Set(_node, _data, -1, std::move(cb));
}

void ZKClient::Set(const std::string& _node, const std::string& _data, sp_int32 _version,
                   VCallback<sp_int32> cb) {
  LOG(INFO) << "Setting zknode " << _node << std::endl;
  sp_int32 rc =
      zoo_aset(zk_handle_, _node.c_str(), _data.c_str(), _data.size(), _version,
               SetCompletionWatcher, CreateCallback(this, &ZKClient::ZkActionCb, std::move(cb)));
  if (rc) {
    // There is nothing we can do here. Continuing will only make
    // other things fail
    LOG(FATAL) << "zoo_aset returned non-zero " << rc << " errno: " << errno << " while setting "
               << _node << "\n";
  }
}

void ZKClient::GetChildren(const std::string& _node, std::vector<std::string>* _children,
                           VCallback<sp_int32> cb) {
  LOG(INFO) << "Getting children for zknode " << _node << std::endl;
  ZKClientGetChildrenStructure* get_structure = new ZKClientGetChildrenStructure();
  get_structure->result_ = _children;
  get_structure->cb_ = CreateCallback(this, &ZKClient::ZkActionCb, std::move(cb));
  sp_int32 rc =
      zoo_aget_children(zk_handle_, _node.c_str(), 0, GetChildrenCompletionWatcher, get_structure);
  if (rc) {
    // There is nothing we can do here. Continuing will only make
    // other things fail
    LOG(FATAL) << "zoo_aget_children returned non-zero " << rc << " errno: " << errno
               << " while getting children of " << _node << "\n";
  }
}

//
// Internal functions
//

// TODO(vikasr): move internal functions to use std::function

// Called when there is some state change wrt zk handle
// Note:- This is called under the context of the zk thread
// So be sure that anything that you do is threadsafe
void ZKClient::GlobalWatcher(zhandle_t* _zh, sp_int32 _type, sp_int32 _state, const char* _path) {
  // Be careful using zk_handler_ here rather than _zzh;
  // the client lib may call the watcher before zookeeper_init returns

  LOG(INFO) << "ZKClient GlobalWatcher called with type " << type2String(_type) << " and state "
            << state2String(_state);
  if (_path && strlen(_path) > 0) {
    LOG(INFO) << " for path " << _path;
  }

  if (_type == ZOO_SESSION_EVENT) {
    if (_state == ZOO_CONNECTED_STATE) {
      const clientid_t* id = zoo_client_id(_zh);
      if (zk_clientid_.client_id == 0 || zk_clientid_.client_id != id->client_id) {
        zk_clientid_ = *id;
        LOG(INFO) << "Got a new session id: " << zk_clientid_.client_id << "\n";
      }
    }
    if (_state == ZOO_AUTH_FAILED_STATE) {
      LOG(FATAL) << "ZKClient Authentication failure. Shutting down...\n";
    } else if (_state == ZOO_EXPIRED_SESSION_STATE) {
      // If client watcher is set, notify it about the session expiry
      // instead of shutting down.
      if (client_global_watcher_cb_) {
        const ZkWatchEvent event = {_type, _state, _path};
        SendWatchEvent(event);
      } else {
        // We need to close and re-establish
        // There are watches, etc that need to be set again.
        // So the simpler option here is to kill ourselves
        LOG(FATAL) << "Session expired. Shutting down...\n";
      }
    } else if (_state == ZOO_CONNECTING_STATE) {
      // We are still in the process of connecting
      LOG(INFO) << "Re-connecting to the zookeeper\n";
    } else if (_state == ZOO_ASSOCIATING_STATE) {
      // Connection process still ongoing
    }
  }
}

// Helper function to init the zk_handle
void ZKClient::InitZKHandle() {
  zk_clientid_.client_id = 0;
  zk_handle_ = zookeeper_init(hostportlist_.c_str(), CallGlobalWatcher, 30000, NULL, this, 0);
  if (!zk_handle_) {
    LOG(FATAL) << "zookeeper_init failed with error " << errno << "\n";
  }
}

void ZKClient::ZkActionCb(sp_int32 rc, VCallback<sp_int32> cb) {
  piper_->ExecuteInEventLoop(std::bind(&RunUserCb, rc, std::move(cb)));
}

void ZKClient::ZkWatcherCb(VCallback<> cb) {
  piper_->ExecuteInEventLoop(std::bind(&RunWatcherCb, std::move(cb)));
}

void ZKClient::SendWatchEvent(const ZkWatchEvent& event) {
  CHECK(client_global_watcher_cb_);
  piper_->ExecuteInEventLoop(std::bind(&RunWatchEventCb, client_global_watcher_cb_, event));
}

const std::string ZKClient::state2String(sp_int32 _state) {
  if (_state == 0) {
    return "CLOSED_STATE";
  } else if (_state == ZOO_CONNECTING_STATE) {
    return "CONNECTING_STATE";
  } else if (_state == ZOO_ASSOCIATING_STATE) {
    return "ASSOCIATING_STATE";
  } else if (_state == ZOO_CONNECTED_STATE) {
    return "CONNECTED_STATE";
  } else if (_state == ZOO_EXPIRED_SESSION_STATE) {
    return "EXPIRED_SESSION_STATE";
  } else if (_state == ZOO_AUTH_FAILED_STATE) {
    return "AUTH_FAILED_STATE";
  } else {
    return "INVALID_STATE";
  }
}

const std::string ZKClient::type2String(sp_int32 _state) {
  if (_state == ZOO_CREATED_EVENT) {
    return "CREATED_EVENT";
  } else if (_state == ZOO_DELETED_EVENT) {
    return "DELETED_EVENT";
  } else if (_state == ZOO_CHANGED_EVENT) {
    return "CHANGED_EVENT";
  } else if (_state == ZOO_CHILD_EVENT) {
    return "CHILD_EVENT";
  } else if (_state == ZOO_SESSION_EVENT) {
    return "SESSION_EVENT";
  } else if (_state == ZOO_NOTWATCHING_EVENT) {
    return "NOTWATCHING_EVENT";
  } else {
    return "UNKNOWN_EVENT_TYPE";
  }
}
