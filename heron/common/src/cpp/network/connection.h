/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HERON_COMMON_SRC_CPP_NETWORK_CONNECTION_H_
#define HERON_COMMON_SRC_CPP_NETWORK_CONNECTION_H_

#include <functional>
#include <list>
#include <utility>
#include "network/packet.h"
#include "network/event_loop.h"
#include "network/baseconnection.h"
#include "network/network_error.h"
#include "basics/basics.h"

/**
 * A Connection is a placeholder for all network and io related
 * information about a socket descriptor. It takes care of async read/write
 * of Packets. Typical ways of instantiating connections are described
 * below in a client and server settings.
 *
 * SERVER
 * After an accept event, the server creates a Connection object giving it
 * an established end point. It then registers for events on this connection
 * like handling a new packet and handling close. It then calls the Start method
 * of the Connection to begin reads/writes from the connection.
 * int fd = accept(...);
 * ConnectionEndPoint* endpoint = new ConnectionEndPoint;
 * ....init endpoint with fd....
 * Connection* conn = new Connection(endpoint, options, ss);
 * conn->registerForNewPacket(...);
 * conn->RegisterForClose(...);
 * conn->Start();
 * After this point, the connection invokes the appropriate callbacks when
 * events occur.
 *
 * CLIENT
 * After a successful connect, the client creates a Connection object giving it
 * an established end point. It register callbacks for events and then invokes
 * Start.
 * int fd = connect(...);
 * ConnectionEndPoint* endpoint = new ConnectionEndPoint;
 * .... init the fd and sockaddr of the endpoint ....
 * Connection* conn = new Connection(endpoint, options, ss);
 * conn->registerForNewPacket(...)
 * conn->RegisterForClose(...);
 * conn->Start();
 * After this point the connection invokes the appropriate callbacks when
 * events occur.
 */
class Connection : public BaseConnection {
 public:
  /**
   * `endpoint` is created by the caller, but now the Connection owns it.
   * `options` is also created by the caller and the caller owns it. options
   *  should be active throught the lifetime of the Connection object.
   */
  Connection(ConnectionEndPoint* endpoint, ConnectionOptions* options, EventLoop* eventLoop);
  virtual ~Connection();

  /**
   * Add this packet to the list of packets to be sent. The packet in itself can be sent
   * later. A zero return value indicates that the packet has been successfully queued to be
   * sent. It does not indicate that the packet was sent successfully. When the packet is
   * actually sent(or determined that it cannot be sent), the callback cb will be called
   * with appropriate status message.
   * A negative value of sendPacket indicates an error. The most likely error is improperly
   * formatted packet.
   * packet should not be touched by the caller until the callback cb has been called.
   */
  sp_int32 sendPacket(OutgoingPacket* packet, VCallback<NetworkErrorCode> cb);

  /**
   * This is the same as above except that we dont need any callback to confirm packet
   * delivery status.
   * packet is owned by the Connection object. It will be deleted after the packet has been
   * written down the wire.
   */
  sp_int32 sendPacket(OutgoingPacket* packet);

  /**
   * Invoke the callback cb when a new packet arrives. A pointer to the packet is passed
   * to the callback cb. That packet is now owned by the callback and is responsible for
   * deleting it.
   */
  void registerForNewPacket(VCallback<IncomingPacket*> cb);

  /**
   * The back pressure starter and reliever are used to communicate to the
   * server whether this connection is under a queue build up or not
   */
  sp_int32 registerForBackPressure(VCallback<Connection*> cbStarter,
                                   VCallback<Connection*> cbReliever);
  /**
   * Invoke this callback when the connection buffer size changes.
   */
  void registerForBufferChange(VCallback<Connection*> cb);

  sp_int32 getOutstandingPackets() const { return mNumOutstandingPackets; }
  sp_int32 getOutstandingBytes() const { return mNumOutstandingBytes; }

  sp_int32 getWriteBatchSize() const { return mWriteBatchsize; }
  void setCausedBackPressure() { mCausedBackPressure = true; }
  void unsetCausedBackPressure() { mCausedBackPressure = false; }
  bool hasCausedBackPressure() const { return mCausedBackPressure; }
  bool isUnderBackPressure() const { return mUnderBackPressure; }

  sp_int32 putBackPressure();
  sp_int32 removeBackPressure();

 public:
  // This is the high water mark on the num of bytes that can be left outstanding on a connection
  static sp_int64 systemHWMOutstandingBytes;
  // This is the low water mark on the num of bytes that can be left outstanding on a connection
  static sp_int64 systemLWMOutstandingBytes;

 private:
  virtual sp_int32 writeIntoEndPoint(sp_int32 fd);

  sp_int32 writeIntoIOVector(sp_int32 maxWrite, sp_int32* toWrite);

  void afterWriteIntoIOVector(sp_int32 simumWrites, ssize_t numWritten);

  virtual bool stillHaveDataToWrite();

  virtual void handleDataWritten();

  virtual sp_int32 readFromEndPoint(sp_int32 _fd);

  virtual void handleDataRead();

  // The list of outstanding packets that need to be sent.
  std::list<std::pair<OutgoingPacket*, VCallback<NetworkErrorCode>>> mOutstandingPackets;
  sp_int64 mNumOutstandingPackets;  // primarily because list's size is linear
  sp_int64 mNumOutstandingBytes;

  // The list of packets that have been sent but not yet been reported to the higher layer
  std::list<std::pair<OutgoingPacket*, VCallback<NetworkErrorCode>>> mSentPackets;

  // The list of packets that have been received but not yet delivered to the higher layer
  std::list<IncomingPacket*> mReceivedPackets;

  // Incompletely read next packet
  IncomingPacket* mIncomingPacket;

  // The user registered callbacks
  VCallback<IncomingPacket*> mOnNewPacket;
  // This call back gets registered from the Server and gets called once the conneciton pipe
  // becomes free (outstanding bytes go to 0)
  VCallback<Connection*> mOnConnectionBufferEmpty;
  // This call back gets registered from the Server and gets called once the conneciton pipe
  // becomes full (outstanding bytes exceed threshold)
  VCallback<Connection*> mOnConnectionBufferFull;

  VCallback<Connection*> mOnConnectionBufferChange;

  sp_int32 mIOVectorSize;
  struct iovec* mIOVector;

  // How many bytes do we want to write in one batch
  sp_int32 mWriteBatchsize;
  // Have we caused back pressure?
  bool mCausedBackPressure;
  // Are our reads being throttled?
  bool mUnderBackPressure;
  // How many times have we enqueued data and found that we had outstanding bytes >
  // HWM of back pressure threshold
  sp_uint8 mNumEnqueuesWithBufferFull;
};

#endif  // HERON_COMMON_SRC_CPP_NETWORK_CONNECTION_H_
