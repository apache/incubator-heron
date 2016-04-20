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

package com.twitter.heron.scheduler.util;

import java.nio.charset.Charset;

import javax.xml.bind.DatatypeConverter;

// Converts the config String argument from Base64 Binary back into the original String in UTF-8.
public class Base64ConfigLoader extends DefaultConfigLoader {
    @Override
    protected String preparePropertyOverride(String configOverride) {
        return new String(DatatypeConverter.parseBase64Binary(configOverride), Charset.forName("UTF-8"));
    }
}
