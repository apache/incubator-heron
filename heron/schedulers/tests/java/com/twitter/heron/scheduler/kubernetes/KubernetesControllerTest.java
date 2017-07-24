// Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.scheduler.kubernetes;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.scheduler.utils.HttpJsonClient;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KubernetesController.class)
public class KubernetesControllerTest {

  private static final String K8S_URI = "http://k8s.uri:8080";
  private static final String NAMESPACE = "default";
  private static final String TOPOLOGY_NAME = "topology_name";
  private static final boolean IS_VERBOSE = true;
  private static final String[] DEPLOY_CONFS = {"test1", "test2"};

  private static KubernetesController controller;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    controller = Mockito.spy(new KubernetesController(K8S_URI,
        NAMESPACE,
        TOPOLOGY_NAME,
        IS_VERBOSE));
  }

  @After
  public void after() throws Exception {
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  /***
   * Test KubernetesController's killTopology method
   * @throws Exception
   */
  @Test
  public void testKillTopology() throws Exception {

    HttpJsonClient httpJsonClient = PowerMockito.spy(new HttpJsonClient(""));
    PowerMockito.whenNew(HttpJsonClient.class).withAnyArguments().thenReturn(httpJsonClient);

    // Test a bad DELETE
    PowerMockito.doThrow(new IOException()).when(httpJsonClient,  "delete", Mockito.anyInt());
    Assert.assertFalse(controller.killTopology());

    // Test a good path
    PowerMockito.doNothing().when(httpJsonClient).delete(Mockito.anyInt());
    Assert.assertTrue(controller.killTopology());
  }

  @Test
  public void testGetBasePod() throws Exception {
    HttpJsonClient httpJsonClient = PowerMockito.spy(new HttpJsonClient(""));
    PowerMockito.whenNew(HttpJsonClient.class).withAnyArguments().thenReturn(httpJsonClient);

    // Test a bad GET
    PowerMockito.doThrow(new IOException()).when(httpJsonClient).get(Mockito.anyInt());
    exception.expect(IOException.class);
    controller.getBasePod(Mockito.anyString());

  }

  @Test
  public void testDeployContainer() throws Exception {
    HttpJsonClient httpJsonClient = PowerMockito.spy(new HttpJsonClient(""));
    PowerMockito.whenNew(HttpJsonClient.class).withAnyArguments().thenReturn(httpJsonClient);

    // Test a bad POST
    PowerMockito.doThrow(new IOException()).when(httpJsonClient).post(Mockito.anyString(),
        Mockito.anyInt());
    exception.expect(IOException.class);
    controller.deployContainer(Mockito.anyString());

  }

  @Test
  public void testRemoveContainer() throws Exception {
    HttpJsonClient httpJsonClient = PowerMockito.spy(new HttpJsonClient(""));
    PowerMockito.whenNew(HttpJsonClient.class).withAnyArguments().thenReturn(httpJsonClient);

    // Test a bad DELETE
    PowerMockito.doThrow(new IOException()).when(httpJsonClient).delete(Mockito.anyInt());
    exception.expect(IOException.class);
    controller.removeContainer(Mockito.anyString());
  }


  /***
   * Test KubernetesController's submitTopology method
   * @throws Exception
   */
  @Test
  public void testSubmitTopology() throws Exception {

    HttpJsonClient httpJsonClient = PowerMockito.spy(new HttpJsonClient(""));
    PowerMockito.whenNew(HttpJsonClient.class).withAnyArguments().thenReturn(httpJsonClient);

    // Test a bad POST
    PowerMockito.doThrow(new IOException()).when(httpJsonClient).post(Mockito.anyString(),
        Mockito.anyInt());
    Assert.assertFalse(controller.submitTopology(DEPLOY_CONFS));

    // Test a good path
    PowerMockito.doNothing().when(httpJsonClient).post(Mockito.anyString(), Mockito.anyInt());
    Assert.assertTrue(controller.submitTopology(DEPLOY_CONFS));

  }
}
