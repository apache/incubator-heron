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

#ifndef SSLOPTIONS_H_
#define SSLOPTIONS_H_

#include <string>
#include "basics/basics.h"

/*
* SSLOptions class definition.
* Specifies options needed to establish an ssl connection using a certificate path
* and a private key path.
*/
class SSLOptions {
 public:
  SSLOptions();
  SSLOptions(const sp_string& certificate_path, const sp_string& private_key_path);
  ~SSLOptions();

  // Filepath to the ssl certificate
  const sp_string& get_certificate_path() const;

  // Filepath to the ssl private key
  const sp_string& get_private_key_path() const;

  // A flag to determine if ssl has been configured. In the default
  // constructor case we consider ssl to not be configured.
  bool is_configured() const;

 private:
  sp_string certificate_path_;
  sp_string private_key_path_;
  bool configured_;
};

#endif  // SSL_OPTIONS_H_
