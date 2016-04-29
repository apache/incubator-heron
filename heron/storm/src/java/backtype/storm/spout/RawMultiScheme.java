// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backtype.storm.spout;

import java.util.List;

import backtype.storm.tuple.Fields;
import static backtype.storm.utils.Utils.tuple;
import static java.util.Arrays.asList;

public class RawMultiScheme implements MultiScheme {
  private static final long serialVersionUID = -8684698009461977572L;

  @Override
  public Iterable<List<Object>> deserialize(byte[] ser) {
    return asList(tuple(ser));
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("bytes");
  }
}
