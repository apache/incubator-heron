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

#include "basics/strutils.h"
#include <cstring>
#include <string>
#include <sstream>
#include <vector>

std::vector<std::string>
StrUtils::split(
  const std::string&         input,
  const std::string&         delim) {
  size_t                    start_pos = 0, pos = 0;
  std::string               atoken;
  std::vector<std::string>  tokens;

  while ((pos = input.find(delim, start_pos)) != std::string::npos) {
    atoken = input.substr(start_pos, pos - start_pos);
    tokens.push_back(atoken);
    start_pos = pos + delim.length();
  }

  if (input.size() > start_pos) {
    atoken = input.substr(start_pos, std::string::npos);
    tokens.push_back(atoken);
  }

  return tokens;
}

// A simple Hex (Base16) encoder
std::vector<char> StrUtils::hexEncode(const std::vector<char>& _input) {
  std::vector<char> output;
  char const hex_chars[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B',
      'C', 'D', 'E', 'F' };
  for (int i = 0; i < _input.size(); ++i) {
      char const byte = _input[i];
      output.push_back(hex_chars[(byte & 0xF0) >> 4]);
      output.push_back(hex_chars[(byte & 0x0F) >> 0]);
  }
  return output;
}

// A simple Hex (Base16) decoder, return decoded vector<char> if success,
// return empty vector<char> if fail.
std::vector<char> StrUtils::hexDecode(const std::vector<char>& _input) {
  std::vector<char> output;
  if (_input.size() % 2 == 1) {
    return output;
  }
  int i = 0;
  while (i + 1 < _input.size()) {
    int chr_1 = decodeHexChar(_input[i]);
    int chr_2 = decodeHexChar(_input[i+1]);
    if (chr_1 == -1 || chr_2 == -1) {
      output.clear();
      return output;
    }
    i += 2;
    char new_chr = ((chr_1 << 4) + chr_2) & 0xFF;
    output.push_back(new_chr);
  }
  return output;
}

// Return decoded value in [0, 15]; return -1 if the character is illegal.
int StrUtils::decodeHexChar(char c) {
  const int char_zero = 48;
  const int char_nine = 57;
  const int char_A = 65;
  const int char_F = 70;

  if (c < char_zero) {
    return -1;
  } else if (c <= char_nine) {
    return c - char_zero;
  } else if (c < char_A) {
    return -1;
  } else if (c <= char_F) {
    return c - char_A + 10;
  } else {
    return -1;
  }
}
