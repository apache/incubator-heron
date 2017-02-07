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
#include "common/fsckpt.h"

namespace heron {
namespace state {

std::string
FSCheckpoint::getFile() {
  std::string filename(getBaseDir());

  // append the topology name
  filename.append("/");
  filename.append(getTopology());

  // append the checkpoint id
  filename.append("/");
  filename.append(getCkptId());

  // append the component id
  filename.append("/");
  filename.append(getComponent());

  // append the instance
  filename.append("/");
  filename.append(getInstance());

  return filename;
}

}  // namespace state
}  // namespace heron
