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

import java.util.Map;
import java.util.Properties;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.IConfigLoader;

/**
 * Loads config in the Java properties file format.
 */
public abstract class AbstractPropertiesConfigLoader implements IConfigLoader {
    public final Properties properties = new Properties();

    protected String preparePropertyOverride(String configOverride) {
        return configOverride;
    }

    protected void addPropertyIfNotPresent(String key, String value) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, value);
        }
    }

    @Override
    public Map<Object, Object> getConfig() {
        return properties;
    }

    @Override
    public String getUploaderClass() {
        return properties.getProperty(Constants.UPLOADER_CLASS);
    }

    @Override
    public String getLauncherClass() {
        return properties.getProperty(Constants.LAUNCHER_CLASS);
    }

    @Override
    public String getSchedulerClass() {
        return properties.getProperty(Constants.SCHEDULER_CLASS);
    }

    @Override
    public String getRuntimeManagerClass() {
        return properties.getProperty(Constants.RUNTIME_MANAGER_CLASS);
    }

    @Override
    public String getPackingAlgorithmClass() {
        return properties.getProperty(Constants.PACKING_ALGORITHM_CLASS);
    }

    @Override
    public String getStateManagerClass() {
        return properties.getProperty(Constants.STATE_MANAGER_CLASS);
    }

    @Override
    public boolean isVerbose() {
        return Boolean.parseBoolean(properties.getProperty(Constants.HERON_VERBOSE));
    }

    public final Properties getProperties() {
        return properties;
    }

    public final String getHeronDir() {
        return properties.getProperty(Constants.HERON_DIR);
    }

    public final String getHeronConfigPath() {
        return properties.getProperty(Constants.HERON_CONFIG_PATH);
    }

    public final void addDefaultProperties() {
        addPropertyIfNotPresent(Constants.HERON_VERBOSE, Boolean.FALSE.toString());
        addPropertyIfNotPresent(Constants.CLUSTER, Constants.CLUSTER);
        addPropertyIfNotPresent(Constants.ROLE, Constants.ROLE);
        addPropertyIfNotPresent(Constants.ENVIRON, Constants.ENVIRON);
    }

    public boolean applyConfigOverride(String configOverride) {
        Properties p = new Properties();
        if (ConfigLoaderUtils.applyPropertyOverride(p, preparePropertyOverride(configOverride))) {
            properties.putAll(p);
            return true;
        } else {
            return false;
        }
    }
}
