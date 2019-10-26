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

////////////////////////////////////////////////////////////////////
//
// Define the class that implements request IDs
//
/////////////////////////////////////////////////////////////////////

#if !defined(__RID_GEN_H)
#define __RID_GEN_H

#include <string>
#include <vector>
#include "basics/sptypes.h"

const sp_uint32 REQID_size = 32;

class REQID {
 public:
  //! Constructors
  REQID() : id_(REQID_size, 0) {}

  //! Destructors
  ~REQID(){};

  //! Overload the assignment operators
  REQID& operator=(const REQID& _reqid) {
    id_ = _reqid.id_;
    return *this;
  }
  void assign(const std::string& _id);

  //! Get the underlying string representation
  const std::string& str() const { return id_; }

  //! Get the underlying C string representation
  const char* c_str() const { return id_.data(); }

  //! Clear the reqid
  void clear() { id_.clear(); }

  //! Get the length of the request id
  static sp_uint32 length() { return REQID_size; }

 private:
  //! Private constructor used by generator
  explicit REQID(const std::string& _id) : id_(_id.c_str(), length()) {}

  //! Underlying representation of request ID
  std::string id_;

  friend bool operator==(const REQID& lhs, const REQID& rhs);
  friend bool operator!=(const REQID& lhs, const REQID& rhs);
  friend std::ostream& operator<<(std::ostream& _os, const REQID& _reqid);

  friend class REQID_Generator;
};

typedef std::vector<REQID> REQID_Vector;
typedef REQID_Vector::iterator REQID_Vector_Iterator;

inline bool operator==(const REQID& lhs, const REQID& rhs) {
  return lhs.id_ == rhs.id_ ? true : false;
}

inline bool operator!=(const REQID& lhs, const REQID& rhs) {
  return lhs.id_ != rhs.id_ ? true : false;
}

inline std::ostream& operator<<(std::ostream& _os, const REQID& _reqid) {
  _os << _reqid.id_;
  return _os;
}

class REQID_Generator {
 public:
  //! Constructor that uses underlying UUID library
  REQID_Generator();

  //! Destructor
  ~REQID_Generator();

  //! Generate a request ID
  REQID generate();

  //! Return a zero REQID
  static REQID generate_zero_reqid();

 private:
  void* rands_;  //! Random stream
};

#endif
