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

#include <string>
#include "basics/basics.h"
#include "network/ssloptions.h"

SSLOptions::SSLOptions() : configured_(false) {}
SSLOptions::SSLOptions(const sp_string &certificate_path, const sp_string &private_key_path)
    : certificate_path_(certificate_path), private_key_path_(private_key_path), configured_(true) {}
SSLOptions::~SSLOptions() {}

const sp_string& SSLOptions::get_certificate_path() const { return certificate_path_; }
const sp_string& SSLOptions::get_private_key_path() const { return private_key_path_; }
bool SSLOptions::is_configured() const { return configured_; }
