/*
 * Copyright 2017 Twitter, Inc.
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

#ifndef __SP_MULTIMAP_H
#define __SP_MULTIMAP_H

#include <functional>
#include <map>
#include <memory>
#include <utility>

template <typename _Key, typename _Tp,
          typename _Compare = std::less<_Key>,
          typename _Alloc = std::allocator<std::pair<const _Key, _Tp>>>
class sp_multimap : public std::multimap<_Key, _Tp, _Compare, _Alloc> {
 public:
  typedef typename std::multimap<_Key, _Tp, _Compare, _Alloc>::value_type value_type;
  typedef typename std::multimap<_Key, _Tp, _Compare, _Alloc>::size_type size_type;

  using std::multimap<_Key, _Tp, _Compare, _Alloc>::erase;

  size_type erase(const value_type& v) {
    auto iterpair = this->equal_range(v.first);
    size_type num = 0;

    for (auto it = iterpair.first; it != iterpair.second;) {
      if (it->second == v.second) {
        it = erase(it);
        num++;
      } else {
        it++;
      }
    }

    return num;
  }
};

#endif
