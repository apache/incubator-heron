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
//
// This file defines the ZKClient class.
// ZKClient is used by all services inside heron to communicate with zoo-keeper.
// ZKClient works over libevent and uses async version of the zk client library
// NOTE:- Currently we use only single threaded zoo-keeper library
//
///////////////////////////////////////////////////////////////////////////////
#ifndef ZKCLIENT_H_
#define ZKCLIENT_H_

#include <zookeeper/proto.h>
#include <zookeeper/zookeeper.h>
#include <string>
#include <vector>
#include "basics/basics.h"
#include "network/network.h"
#include "threads/threads.h"

/*
 * ZKClient class definition
 */
class ZKClient {
 public:
  // Helper struct to pass watch events
  struct ZkWatchEvent {
    sp_int32 type;
    sp_int32 state;
    std::string path;
  };

  // Constructor/Destructor
  ZKClient(const std::string& hostportlist, std::shared_ptr<EventLoop> eventLoop);

  // If a global_watcher is provided, the clients can watch on session events.
  // Right now only SessionExpired event is notified, but could have others
  // in the future. global_watcher_cb should be a PermanentCallback.
  ZKClient(const std::string& hostportlist, std::shared_ptr<EventLoop> eventLoop,
           VCallback<ZkWatchEvent> global_watcher_cb);

  virtual ~ZKClient();

  // Queries whether the node exists in the zk. If _watcher is not null
  // sets a watcher that will be called when the node changes.
  // _cb is called with the
  // status code after this call completes. A ZOK status code means
  // that the node exists. Otherwise there was some error
  virtual void Exists(const std::string& _node, VCallback<> _watcher, VCallback<sp_int32> _cb);
  // same as above if we are not interested in watching
  virtual void Exists(const std::string& _node, VCallback<sp_int32> _cb);

  // creates a node. The node is created at _node. If _is_ephemeral is set,
  // then the node is created as a ephemeral node. _cb is called with
  // the status code after the Create completes.
  virtual void CreateNode(const std::string& _node, const std::string& _value, bool _is_ephimeral,
                          VCallback<sp_int32> _cb);

  // deletes a node. _cb is called with the status code after Delete completes
  virtual void DeleteNode(const std::string& _node, VCallback<sp_int32> _cb);

  // Gets the data at a node. The data is filled in the _data provided
  // by the caller. If _version is not null, we store the version there.
  // If _watcher is not null, we set a watch on this node and call the _watcher
  // function when the node changes.
  // _cb is called with the result code after get completes.
  virtual void Get(const std::string& _node, std::string* _data, sp_int32* _version,
                   VCallback<> _watcher, VCallback<sp_int32> _cb);
  virtual void Get(const std::string& _node, std::string* _data, sp_int32* _version,
                   VCallback<sp_int32> _cb);
  // Same as above except we dont care about the version
  virtual void Get(const std::string& _node, std::string* _data, VCallback<sp_int32> _cb);

  // Sets the data at a node.
  virtual void Set(const std::string& _node, const std::string& _data, sp_int32 _version,
                   VCallback<sp_int32> _cb);
  // Same as above except brute force the write
  virtual void Set(const std::string& _node, const std::string& _data, VCallback<sp_int32> _cb);

  // lists all the children. The children will be filled at _children list created
  // by the caller.
  virtual void GetChildren(const std::string& _node, std::vector<std::string>* _children,
                           VCallback<sp_int32> _cb);

  // friend functions
  friend void CallGlobalWatcher(zhandle_t* _zh, sp_int32 _type, sp_int32 _state, const char* _path,
                                void* _context);
  friend void StringCompletionWatcher(sp_int32 _rc, const char* _name, const void* _data);
  friend void VoidCompletionWatcher(sp_int32 _rc, const void* _data);
  friend void GetCompletionWatcher(sp_int32 _rc, const char* _value, int _value_len,
                                   const struct Stat* _stat, const void* _data);
  friend void SetCompletionWatcher(sp_int32 _rc, const struct Stat* _stat, const void* _data);
  friend void GetChildrenCompletionWatcher(sp_int32 _rc, const struct String_vector* _strings,
                                           const void* _data);

  // Util functions to convert from the codes to strings
  static const std::string type2String(sp_int32 state);
  static const std::string state2String(sp_int32 state);

 protected:
  // A empty constructor for test purposes ONLY. Enables us to create a
  // MockZkClient without worrying about the actual private member variables.
  ZKClient()
      : zk_handle_(NULL),
        eventLoop_(NULL),
        client_global_watcher_cb_(VCallback<ZkWatchEvent>()) {}

 private:
  // the global watcher
  void GlobalWatcher(zhandle_t* _zh, sp_int32 _type, sp_int32 _state, const char* _path);

  // The function that actually inits the handle
  void InitZKHandle();

  // We wrap all user zk calls with this completion function
  // This completion function runs in the context of the
  // zk completion thread. It basically calls the piper
  // to execute the cb in eventLoop thread
  void ZkActionCb(sp_int32 rc, VCallback<sp_int32> cb);

  // This is the watcher function that gets called
  // when a node changes
  void ZkWatcherCb(VCallback<> cb);

  // Common functionality for c`tors. Should be called only once from c`tor
  void Init();
  // Sends events to clients, if client global watcher is set.
  void SendWatchEvent(const ZkWatchEvent& event);

  clientid_t zk_clientid_;
  zhandle_t* zk_handle_;
  std::shared_ptr<EventLoop> eventLoop_;
  std::string hostportlist_;

  // We use libzookeeper_mt as our zk library. This means that
  // zk callbacks are all executed in the context of a zk thread.
  // Piper are how they communicate it accross to our main thread
  Piper* piper_;
  // A callback to notify the clients of this class about global session events.
  VCallback<ZkWatchEvent> client_global_watcher_cb_;
};

#endif  // ZKCLIENT_H_
