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

package com.twitter.heron.scheduler.twitter;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

public class PackerUploaderTest {
    public static final String cluster = "cluster";
    public static final String role = "me";
    public static final String pkgName = "pkg";
    public static final String stateMgrClass = "com.twitter.heron.statemgr.NullStateManager";

    public static DefaultConfigLoader getRequiredConfig() throws Exception {
        DefaultConfigLoader configLoader = DefaultConfigLoader.class.newInstance();
        configLoader.addDefaultProperties();
        configLoader.properties.setProperty(Constants.CLUSTER, cluster);
        configLoader.properties.setProperty(Constants.ROLE, role);
        configLoader.properties.setProperty(Constants.HERON_RELEASE_PACKAGE_NAME, pkgName);
        configLoader.properties.setProperty(Constants.STATE_MANAGER_CLASS, stateMgrClass);
        return configLoader;
    }

    @Test
    public void testPackerUploader() throws Exception {
        String topologyPkg = "someTopologyPkg.tar";
        TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
                "name", new com.twitter.heron.api.Config(), new HashMap<String, Integer>(), new HashMap<String, Integer>());
        PackerUploader uploader = Mockito.spy(PackerUploader.class.newInstance());
        Mockito.doReturn(0).when(uploader).runProcess(Matchers.anyString(), Matchers.any(StringBuilder.class));
        Mockito.doReturn("").when(uploader).getTopologyURI(Matchers.anyString());
        DefaultConfigLoader config = getRequiredConfig();
        LaunchContext context =
                new LaunchContext(config, topology);
        uploader.initialize(context);
        Assert.assertTrue(uploader.uploadPackage(topologyPkg));
        String expectedPackerAddCmd = String.format("packer add_version --cluster %s %s %s %s --json",
                cluster, role, uploader.getTopologyPackageName(), topologyPkg);
        Mockito.verify(uploader).runProcess(Matchers.eq(expectedPackerAddCmd), Matchers.any(StringBuilder.class));
        String expectedPackerSetLiveCmd = String.format("packer set_live --cluster %s %s %s latest",
                cluster, role, uploader.getTopologyPackageName());
        Mockito.verify(uploader).runProcess(Matchers.eq(expectedPackerSetLiveCmd), Matchers.any(StringBuilder.class));
    }

    @Test
    public void testPackerUploaderFail() throws Exception {
        String topologyPkg = "someTopologyPkg.tar";
        TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
                "name", new com.twitter.heron.api.Config(), new HashMap<String, Integer>(), new HashMap<String, Integer>());
        PackerUploader uploader = Mockito.spy(PackerUploader.class.newInstance());

        // packer add_version will return fail.
        Mockito.doReturn(0).when(uploader).runProcess(Matchers.anyString(), Matchers.any(StringBuilder.class));
        Mockito.doReturn(1).when(uploader).runProcess(Matchers.startsWith("packer add_version"), Matchers.any(StringBuilder.class));
        Mockito.doReturn("").when(uploader).getTopologyURI(Matchers.anyString());

        DefaultConfigLoader config = getRequiredConfig();
        LaunchContext context =
                new LaunchContext(config, topology);
        uploader.initialize(context);
        Assert.assertFalse(uploader.uploadPackage(topologyPkg));
        String expectedPackerAddCmd = String.format("packer add_version --cluster %s %s %s %s --json",
                cluster, role, uploader.getTopologyPackageName(), topologyPkg);
        Mockito.verify(uploader).runProcess(Matchers.eq(expectedPackerAddCmd), Matchers.any(StringBuilder.class));
        String expectedPackerSetLiveCmd = String.format("packer set_live --cluster %s %s %s latest",
                cluster, role, uploader.getTopologyPackageName());
        Mockito.verify(uploader, Mockito.never()).runProcess(Matchers.eq(expectedPackerSetLiveCmd), Matchers.any(StringBuilder.class));
    }
}
