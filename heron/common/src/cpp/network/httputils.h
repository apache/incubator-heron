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
// This file defines the HTTP request and response objects
///////////////////////////////////////////////////////////////////////////////

#ifndef HTTPUTILS_H_
#define HTTPUTILS_H_

#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>
#include "basics/basics.h"

// Fowrard Declarations
struct evhttp_request;
class OutgoingHTTPResponse;

typedef std::vector<std::pair<sp_string, sp_string> > HTTPKeyValuePairs;

class BaseHTTPRequest {
 public:
  enum HTTPRequestType { GET = 0, POST, HEAD, PUT, DELETE, UNKNOWN };

  explicit BaseHTTPRequest(HTTPRequestType _type);
  virtual ~BaseHTTPRequest();

  HTTPRequestType type() const { return type_; }
  void set_type(HTTPRequestType _type) { type_ = _type; }

  static sp_string http_decode(const sp_string& _value);
  static sp_string http_encode(const sp_string& _value);

  static sp_string parse_query(const char* _uri);
  static void parse_keyvalues(HTTPKeyValuePairs& _kv, const char* _data);
  static sp_string encode_keyvalues(std::unordered_map<sp_string, sp_string>& _kv);

  static bool ExtractHostNameAndQuery(const sp_string& _url, sp_string& _host, sp_int32& _port,
                                      sp_string& _query);

 private:
  HTTPRequestType type_;
  static std::unordered_map<char, sp_string> httpencodemap_;
};

class IncomingHTTPRequest : public BaseHTTPRequest {
 public:
  explicit IncomingHTTPRequest(struct evhttp_request* _request);
  virtual ~IncomingHTTPRequest();

  //! Get the uri
  const sp_string& GetQuery() const;

  //! Get the key value pairs
  const HTTPKeyValuePairs& keyvalues() const { return kv_; }

  //! Get the value of a particular key
  const sp_string& GetValue(const sp_string& _key) const;

  //! Get the all the values of a particular key
  bool GetAllValues(const sp_string& _key, std::vector<sp_string>& _values) const;

  //! Sets the value. Note that the underlying object is not touched
  void AddValue(const sp_string& _key, const sp_string& _value);

  //! get the value of a particular header key
  const sp_string GetHeader(const sp_string& _key);

  struct evhttp_request* underlying_request() {
    return request_;
  }

  unsigned char* ExtractFromPostData(sp_int32 _start, sp_uint32 _length);

  // IP address and port for the source endpoint of this request
  sp_string GetRemoteHost();
  sp_int32 GetRemotePort();

  // Calculate the entire request size.
  // Includes the size of the uri + headers + payload if any
  sp_int32 GetRequestSize();

  // Calculate just the size of the payload
  sp_int32 GetPayloadSize();

 private:
  friend class OutgoingHTTPResponse;

  //! The parsed query
  sp_string query_;
  //! The parsed key value pairs
  HTTPKeyValuePairs kv_;

  //! The underlying libevent structure
  struct evhttp_request* request_;
};

class OutgoingHTTPRequest : public BaseHTTPRequest {
 public:
  OutgoingHTTPRequest(const sp_string& _host, sp_int32 _port, const sp_string& _uri,
                      BaseHTTPRequest::HTTPRequestType _type, const HTTPKeyValuePairs& _kvs);
  virtual ~OutgoingHTTPRequest();

  //! Sets the value of the key
  void AddValue(const sp_string& _key, const sp_string& _value);

  //! sets the header
  void SetHeader(const sp_string& _key, const sp_string& _value);

  //! Appends the key value pairs to the uri
  void ExtendQuery(const sp_string& _body);

  //! Accessor functions
  const sp_string& host() const { return host_; }
  sp_int32 port() const { return port_; }
  const sp_string& query() const { return query_; }
  const std::unordered_map<sp_string, sp_string>& header() const { return header_; }
  const HTTPKeyValuePairs& kv() const { return kv_; }

 private:
  sp_string host_;
  sp_int32 port_;
  sp_string query_;
  //! The key value pairs
  HTTPKeyValuePairs kv_;
  std::unordered_map<sp_string, sp_string> header_;
};

class OutgoingHTTPResponse {
 public:
  //! Constructors/Destructors
  explicit OutgoingHTTPResponse(IncomingHTTPRequest* _request);
  virtual ~OutgoingHTTPResponse();

  //! Adds string to the response body
  void AddResponse(const sp_string& _str);

  //! Adds header
  void AddHeader(const sp_string& _key, const sp_string& _val);

  struct evhttp_request* underlying_response() {
    return response_;
  }

 private:
  //! The underliying request structure
  struct evhttp_request* response_;
};

class IncomingHTTPResponse {
 public:
  explicit IncomingHTTPResponse(struct evhttp_request* _response);
  virtual ~IncomingHTTPResponse();

  //! get the value of a particular header key
  sp_string header(const sp_string& _key);

  //! Get the response body
  const sp_string& body() const { return body_; }

  //! Get the response status code
  sp_int32 response_code() const { return response_code_; }

 private:
  sp_int32 response_code_;
  std::unordered_map<sp_string, sp_string> headers_;
  sp_string body_;
};

#endif  // HTTPUTILS_H_
