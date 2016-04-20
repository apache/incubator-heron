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

import org.junit.Assert;
import org.junit.Test;

public class Base64ConfigLoaderTest {
    @Test
    public void testBase64DecodedOverride() throws Exception {
        Base64ConfigLoader loader = Base64ConfigLoader.class.newInstance();
        loader.applyConfigOverride(DatatypeConverter.printBase64Binary(
                "key=value".getBytes(Charset.forName("UTF-8"))));
        Assert.assertEquals("value", loader.properties.getProperty("key"));
    }
}
