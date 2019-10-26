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

////////////////////////////////////////////////////////////////////////////////
// Implements the HTTPRequest and HTTPResponse functions and some utility
// functions
////////////////////////////////////////////////////////////////////////////////

#include "network/httputils.h"
#include <stdlib.h>
#include <sys/queue.h>
#include <evhttp.h>
#include <string>
#include <vector>
#include "glog/logging.h"

std::unordered_map<char, sp_string> PopulateEncodeMap() {
  std::unordered_map<char, sp_string> retval;
  retval[' '] = "%20";
  retval['<'] = "%3C";
  retval['>'] = "%3E";
  retval['#'] = "%23";
  retval['%'] = "%25";
  retval['{'] = "%7B";
  retval['}'] = "%7D";
  retval['|'] = "%7C";
  retval['\\'] = "%5C";
  retval['^'] = "%5E";
  retval['~'] = "%7E";
  retval['['] = "%5B";
  retval[']'] = "%5D";
  retval['`'] = "%60";
  retval[';'] = "%3B";
  retval['/'] = "%2F";
  retval['?'] = "%3F";
  retval[':'] = "%3A";
  retval['@'] = "%40";
  retval['='] = "%3D";
  retval['&'] = "%26";
  retval['$'] = "%24";
  retval['"'] = "%22";
  return retval;
}

std::unordered_map<char, sp_string> BaseHTTPRequest::httpencodemap_ = PopulateEncodeMap();

static sp_string ExtractHostAndPort(const sp_string& _host, sp_int32& _port) {
  size_t sz = _host.find(":");
  if (sz == std::string::npos) {
    _port = 80;
    return _host;
  } else {
    sp_string retval = std::string(_host, 0, sz);
    if (_host.size() == sz + 1) {
      _port = 80;
    } else {
      _port = atoi(_host.c_str() + sz + 1);
    }
    return retval;
  }
}

BaseHTTPRequest::BaseHTTPRequest(HTTPRequestType _type) { type_ = _type; }

BaseHTTPRequest::~BaseHTTPRequest() {}

BaseHTTPRequest::HTTPRequestType GetRequestType(evhttp_cmd_type _type) {
  switch (_type) {
    case EVHTTP_REQ_GET:
      return BaseHTTPRequest::GET;
    case EVHTTP_REQ_POST:
      return BaseHTTPRequest::POST;
    case EVHTTP_REQ_HEAD:
      return BaseHTTPRequest::HEAD;
    case EVHTTP_REQ_PUT:
      return BaseHTTPRequest::PUT;
    case EVHTTP_REQ_DELETE:
      return BaseHTTPRequest::DELETE;
    default:
      return BaseHTTPRequest::UNKNOWN;
  }
}

// decode a possibly encoded value passed via http.  Based loosely on
// evhttp_decode_uri from libevent but is modified because of several
// bugs in the libevent code.
sp_string BaseHTTPRequest::http_decode(const sp_string& value) {
  char c;
  sp_string retval;

  for (size_t i = 0; i < value.size(); ++i) {
    c = value[i];
    if (c == '+') {
      retval.push_back(' ');
    } else if (c == '%') {
      if ((i + 2) < value.size() && isxdigit((unsigned char)value[i + 1]) &&
          isxdigit((unsigned char)value[i + 2])) {
        char tmp[] = {value[i + 1], value[i + 2], '\0'};
        retval.push_back(static_cast<char>(strtol(tmp, NULL, 16)));
        i += 2;
      } else {
        // if we got a '%' but there is not enough chars left in the string
        // or it is not a encoded value
        retval.push_back(c);
      }
    } else {
      retval.push_back(c);
    }
  }
  // retval.push_back('\0');
  return std::string(retval);
}

sp_string BaseHTTPRequest::http_encode(const sp_string& _value) {
  sp_string retval;
  for (size_t i = 0; i < _value.size(); ++i) {
    if (httpencodemap_.find(_value[i]) == httpencodemap_.end()) {
      retval.push_back(_value[i]);
    } else {
      retval.append(httpencodemap_[_value[i]]);
    }
  }
  return retval;
}

// the evhttp code has a bug in it where it decodes the URI string before it
// checks for invalid line feeds... which stops us from handling 'textarea'
// form entries that are multi-line.  So we will use our own.  Based on the
// code in libevent/http.c but customised.
sp_string BaseHTTPRequest::parse_query(const char* _uri) {
  std::string parsed_uri;
  if (strchr(_uri, '?') == NULL)
    parsed_uri = http_decode(std::string(_uri));
  else
    parsed_uri = http_decode(std::string(_uri, strchr(_uri, '?') - _uri));
  return parsed_uri;
}

void BaseHTTPRequest::parse_keyvalues(HTTPKeyValuePairs& _params, const char* _data) {
  const char* argument = _data;

  while (argument != NULL && *argument != '\0') {
    const char* value = strchr(argument, '=');
    if (!value) return;
    sp_string key = std::string(argument, value - argument);
    key = http_decode(key);
    value++;  // move past =
    if (!value || *value == '\0') {
      _params.push_back(make_pair(key, ""));
      return;
    }
    const char* next = strchr(value, '&');
    if (!next) {
      sp_string val = http_decode(value);
      _params.push_back(make_pair(key, val));
      return;
    }
    sp_string val = http_decode(std::string(value, next - value));
    _params.push_back(make_pair(key, val));
    argument = next + 1;
  }
  return;
}

sp_string BaseHTTPRequest::encode_keyvalues(std::unordered_map<sp_string, sp_string>& _kv) {
  std::ostringstream encoded;
  auto kvpair = _kv.begin();
  encoded << http_encode(kvpair->first) << "=" << http_encode(kvpair->second);

  for (; kvpair != _kv.end(); kvpair++) {
    encoded << "&" << http_encode(kvpair->first) << "=" << http_encode(kvpair->second);
  }

  return encoded.str();
}

bool BaseHTTPRequest::ExtractHostNameAndQuery(const sp_string& _url, sp_string& _host,
                                              sp_int32& _port, sp_string& _uri) {
  const sp_string prefix = "http://";
  size_t sz = _url.find(prefix);
  if (sz != 0) return false;
  sz += prefix.size();
  sz = _url.find("/", sz);
  if (sz == std::string::npos) {
    _host = std::string(_url, prefix.size(), _url.size() - prefix.size());
    _uri = "/";
    _host = ExtractHostAndPort(_host, _port);
    if (_host.empty()) return false;
    return true;
  } else {
    _host = std::string(_url, prefix.size(), sz - prefix.size());
    _uri = std::string(_url, sz, _url.size() - sz);
    _host = ExtractHostAndPort(_host, _port);
    if (_host.empty()) return false;
    return true;
  }
}

IncomingHTTPRequest::IncomingHTTPRequest(struct evhttp_request* _request)
    : BaseHTTPRequest(GetRequestType(_request->type)) {
  request_ = _request;
  query_ = parse_query(request_->uri);
  if (request_->type == EVHTTP_REQ_POST || request_->type == EVHTTP_REQ_PUT) {
    const char* content_type = evhttp_find_header(_request->input_headers, "Content-Type");
    if (!content_type || strcmp(content_type, "application/x-www-form-urlencoded") == 0) {
      char* bufdata =  reinterpret_cast<char*>(EVBUFFER_DATA(request_->input_buffer));
      size_t buflen = EVBUFFER_LENGTH(request_->input_buffer);
      std::string body = std::string(bufdata, buflen);
      parse_keyvalues(kv_, body.c_str());
    } else {
      // Unlike most HTTP Servers, we still will parse post's parameters
      if (strchr(request_->uri, '?') != NULL) {
        parse_keyvalues(kv_, strchr(request_->uri, '?') + 1);
      }
    }
  } else {
    if (strchr(request_->uri, '?') != NULL) {
      parse_keyvalues(kv_, strchr(request_->uri, '?') + 1);
    }
  }
}

IncomingHTTPRequest::~IncomingHTTPRequest() {}

const sp_string& IncomingHTTPRequest::GetQuery() const { return query_; }

const sp_string& IncomingHTTPRequest::GetValue(const sp_string& _key) const {
  for (size_t i = 0; i < kv_.size(); ++i) {
    if (kv_[i].first == _key) {
      return kv_[i].second;
    }
  }
  return EMPTY_STRING;
}

bool IncomingHTTPRequest::GetAllValues(const sp_string& _key,
                                       std::vector<sp_string>& _values) const {
  bool retval = false;
  for (size_t i = 0; i < kv_.size(); ++i) {
    if (kv_[i].first == _key) {
      _values.push_back(kv_[i].second);
      retval = true;
    }
  }
  return retval;
}

void IncomingHTTPRequest::AddValue(const sp_string& _key, const sp_string& _value) {
  kv_.push_back(make_pair(_key, _value));
}

// returns the value of the given header attribute
const sp_string IncomingHTTPRequest::GetHeader(const sp_string& _key) {
  const char* header = evhttp_find_header(request_->input_headers, _key.c_str());
  if (header) {
    return header;
  } else {
    return EMPTY_STRING;
  }
}

unsigned char* IncomingHTTPRequest::ExtractFromPostData(sp_int32 _start, sp_uint32 _len) {
  unsigned char* contig_buffer = evbuffer_pullup(request_->input_buffer, _start + _len);
  return contig_buffer + _start;
}

// IP address of the source endpoint of this request
sp_string IncomingHTTPRequest::GetRemoteHost() {
  sp_string x_forwarded_for = GetHeader("X-Forwarded-For");
  if (x_forwarded_for.empty()) {
    return request_->remote_host;
  } else {
    if (x_forwarded_for.find(',') == std::string::npos) {
      return x_forwarded_for;
    } else {
      return std::string(x_forwarded_for, 0, x_forwarded_for.find(','));
    }
  }
}

// Port of the source endpoint of this request
sp_int32 IncomingHTTPRequest::GetRemotePort() { return request_->remote_port; }

// Calculate the total payload size for the request
sp_int32 IncomingHTTPRequest::GetRequestSize() {
  std::string uri = request_->uri;
  sp_int32 uri_size = uri.length() * sizeof(char);

  evkeyval* p;
  p = TAILQ_FIRST(request_->input_headers);
  sp_int32 headers_size = 0;
  while (p) {
    std::string hkey = p->key;
    std::string hval = p->value;
    headers_size += (hkey.length() * sizeof(char)) + (hval.length() * sizeof(char));
    p = TAILQ_NEXT(p, next);
  }

  return uri_size + headers_size + EVBUFFER_LENGTH(request_->input_buffer);
}

sp_int32 IncomingHTTPRequest::GetPayloadSize() { return EVBUFFER_LENGTH(request_->input_buffer); }

OutgoingHTTPRequest::OutgoingHTTPRequest(const sp_string& _host, sp_int32 _port,
                                         const sp_string& _uri,
                                         BaseHTTPRequest::HTTPRequestType _type,
                                         const HTTPKeyValuePairs& _kvs)
    : BaseHTTPRequest(_type) {
  host_ = _host;
  port_ = _port;
  query_ = _uri;
  kv_ = _kvs;
}

OutgoingHTTPRequest::~OutgoingHTTPRequest() {}

void OutgoingHTTPRequest::AddValue(const sp_string& _key, const sp_string& _value) {
  kv_.push_back(make_pair(_key, _value));
}

void OutgoingHTTPRequest::SetHeader(const sp_string& _key, const sp_string& _value) {
  header_[_key] = _value;
}

void OutgoingHTTPRequest::ExtendQuery(const sp_string& _body) { query_ += "?" + _body; }

OutgoingHTTPResponse::OutgoingHTTPResponse(IncomingHTTPRequest* _request)
    : response_(_request->request_) {}

OutgoingHTTPResponse::~OutgoingHTTPResponse() {}

void OutgoingHTTPResponse::AddResponse(const sp_string& _str) {
  CHECK_GE(evbuffer_add_printf(response_->output_buffer, "%s", _str.c_str()), 0);
}

void OutgoingHTTPResponse::AddHeader(const sp_string& _key, const sp_string& _value) {
  CHECK_EQ(evhttp_add_header(response_->output_headers, _key.c_str(), _value.c_str()), 0);
}

IncomingHTTPResponse::IncomingHTTPResponse(struct evhttp_request* _response) {
  response_code_ = -1;
  if (_response) {
    response_code_ = _response->response_code;
    char* bufdata = reinterpret_cast<char*>(EVBUFFER_DATA(_response->input_buffer));
    size_t buflen = EVBUFFER_LENGTH(_response->input_buffer);
    body_ = std::string(bufdata, buflen);
    struct evkeyval* header;
    TAILQ_FOREACH(header, _response->input_headers, next) {
      sp_string hdr = header->key;
      for (size_t i = 0; i < hdr.size(); ++i) {
        hdr[i] = tolower(hdr[i]);
      }
      headers_[hdr] = header->value;
    }
  }
}

IncomingHTTPResponse::~IncomingHTTPResponse() {}

sp_string IncomingHTTPResponse::header(const sp_string& _key) {
  sp_string key;
  for (size_t i = 0; i < _key.size(); ++i) {
    key.push_back(tolower(_key[i]));
  }
  if (headers_.find(key) == headers_.end()) return EMPTY_STRING;
  return headers_[key];
}
