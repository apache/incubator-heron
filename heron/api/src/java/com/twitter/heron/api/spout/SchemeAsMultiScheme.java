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

package com.twitter.heron.api.spout;

import java.util.Arrays;
import java.util.List;

import com.twitter.heron.api.tuple.Fields;

public class SchemeAsMultiScheme implements MultiScheme {
    public final Scheme scheme;

    public SchemeAsMultiScheme(Scheme scheme) {
        this.scheme = scheme;
    }

    @Override
    public Iterable<List<Object>> deserialize(final byte[] ser) {
        List<Object> o = scheme.deserialize(ser);
        if (o == null) return null;
        else return Arrays.asList(o);
    }

    @Override
    public Fields getOutputFields() {
        return scheme.getOutputFields();
    }
}
