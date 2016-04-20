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

package com.twitter.heron.scheduler.aurora;

import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.spi.common.Constants;

public class AuroraConfigLoaderTest {
    private static final Logger LOG = Logger.getLogger(AuroraConfigLoaderTest.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private void addConfig(StringBuilder builder, String key, String value) {
        builder.append(String.format(" %s=\"%s\"", key, value));
    }

    @Test
    public void testAuroraOverrides() throws Exception {
        String override = "cluster=cluster role=role environ=environ";
        AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
        // Disables version check
        configLoader.properties.setProperty(Constants.HERON_RELEASE_PACKAGE_NAME, "test");
        configLoader.applyConfigOverride(override);
        Assert.assertEquals("cluster", configLoader.properties.getProperty(Constants.CLUSTER));
        Assert.assertEquals("environ", configLoader.properties.getProperty(Constants.ENVIRON));
    }

    @Test
    public void testAuroraOverridesWithDefaultOverrides() throws Exception {
        String override = "cluster=cluster role=role environ=environ key1=value1 key2=value2";
        AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
        configLoader.properties.setProperty(Constants.HERON_RELEASE_PACKAGE_NAME, "test");
        configLoader.applyConfigOverride(override);
        Assert.assertEquals("cluster", configLoader.properties.getProperty(Constants.CLUSTER));
        Assert.assertEquals("role", configLoader.properties.getProperty(Constants.ROLE));
        Assert.assertEquals("environ", configLoader.properties.getProperty(Constants.ENVIRON));
        Assert.assertEquals("value1", configLoader.properties.getProperty("key1"));
        Assert.assertEquals("value2", configLoader.properties.getProperty("key2"));
    }

    @Test
    public void testAuroraRespectRespectHeronVersion() throws Exception {
        StringBuilder override = new StringBuilder("cluster=cluster role=role environ=environ");

        // Add required heron package defaults
        addConfig(override, Constants.HERON_RELEASE_PACKAGE_NAME, "testPackage");
        addConfig(override, Constants.HERON_RELEASE_PACKAGE_ROLE, "test");
        addConfig(override, Constants.HERON_RELEASE_PACKAGE_VERSION, "live");

        AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
        configLoader.applyConfigOverride(override.toString());
        // Verify translated package
        Assert.assertEquals("live",
                configLoader.properties.getProperty(Constants.HERON_RELEASE_PACKAGE_VERSION));
    }
}
