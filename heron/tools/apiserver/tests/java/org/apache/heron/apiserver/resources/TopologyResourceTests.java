/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.apiserver.resources;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.apiserver.actions.Action;
import org.apache.heron.apiserver.actions.ActionFactory;
import org.apache.heron.apiserver.actions.ActionType;
import org.apache.heron.spi.common.Config;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TopologyResourceTests {

  private static final int HTTP_422 = 422;

  private String cluster = "cluster";
  private String role = "role";
  private String environment = "environment";
  private String topologyName = "topology";

  private Action action;
  private ActionFactory factory;
  private TopologyResource resource;

  private final List<String> requiredSubmitParamKeys = Arrays.asList(
      "name", "cluster", "role", "definition", "topology"
  );

  @Before
  public void before() {
    action = mock(Action.class);
    resource = spy(new TopologyResource());
    factory = spy(createFactory(action));
    doReturn(factory).when(resource).getActionFactory();
    doReturn(Config.newBuilder().build()).when(resource).getBaseConfiguration();
    doReturn(cluster).when(resource).getCluster();
  }

  @Test
  public void testVerifyKeys() {
    assertEquals(1,
        TopologyResource.verifyKeys(new HashSet<>(), "key").size());

    assertEquals(0,
        TopologyResource.verifyKeys(new HashSet<>(Arrays.asList("key1")),
            "key1").size());
  }

  @Test
  public void testSubmitMissingParams() throws IOException {
    FormDataMultiPart form = mock(FormDataMultiPart.class);
    for (final String paramKey : requiredSubmitParamKeys) {
      Set<String> keySet =
          requiredSubmitParamKeys
              .stream()
              .filter(s -> s.equals(paramKey))
              .collect(Collectors.toSet());

      Map<String, List<FormDataBodyPart>> map = new HashMap<>();
      keySet.forEach(t -> map.put(t, Collections.emptyList()));
      when(form.getFields()).thenReturn(map);

      Response response = resource.submit(form);
      assertEquals(HTTP_422, response.getStatus());
    }
  }

  @Test
  public void testKillOk() {
    final Response response = resource.kill(cluster, role, environment, topologyName);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
  }

  @Test
  public void testOnKillActionCalled() {
    resource.kill(cluster, role, environment, topologyName);
    verify(factory, times(1))
        .createRuntimeAction(any(Config.class), eq(ActionType.KILL));
    verify(action, times(1)).execute();
  }

  @Test
  public void testActivateOk() {
    final Response response = resource.activate(cluster, role, environment, topologyName);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
  }

  @Test
  public void testActivateActionCalled() {
    resource.activate(cluster, role, environment, topologyName);
    verify(factory, times(1))
        .createRuntimeAction(any(Config.class), eq(ActionType.ACTIVATE));
    verify(action, times(1)).execute();
  }

  @Test
  public void testDeactivateOk() {
    final Response response = resource.deactivate(cluster, role, environment, topologyName);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
  }

  @Test
  public void testDeactivateActionCalled() {
    resource.deactivate(cluster, role, environment, topologyName);
    verify(factory, times(1))
        .createRuntimeAction(any(Config.class), eq(ActionType.DEACTIVATE));
    verify(action, times(1)).execute();
  }

  @Test
  public void testRestartOk() {
    final Response response = resource.restart(cluster, role, environment, topologyName, -1);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
  }

  @Test
  public void testRestartActionCalled() {
    resource.restart(cluster, role, environment, topologyName, -1);
    verify(factory, times(1))
        .createRuntimeAction(any(Config.class), eq(ActionType.RESTART));
    verify(action, times(1)).execute();
  }

  @Test
  public void testUpdateOk() {
    MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
    params.putSingle("component_parallelism", "word:1");
    Response response = resource.update(cluster, role, environment, topologyName, params);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testUpdateActionCalled() {
    MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
    params.putSingle("component_parallelism", "word:1");
    resource.update(cluster, role, environment, topologyName, params);
    verify(factory, times(1))
        .createRuntimeAction(any(Config.class), eq(ActionType.UPDATE));
    verify(action, times(1)).execute();
  }

  @Test
  public void testUpdateMissingParams() {
    Response response = resource.update(cluster, role, environment, topologyName, null);
    assertEquals(HTTP_422, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
    response = resource.update(cluster, role, environment, topologyName, params);
    assertEquals(HTTP_422, response.getStatus());
  }

  private ActionFactory createFactory(Action a) {
    return new TestActionFactory(a);
  }

  private static class TestActionFactory implements ActionFactory {

    private final Action action;

    TestActionFactory(Action a) {
      action = a;
    }

    @Override
    public Action createSubmitAction(Config config, String topologyPackagePath,
          String topologyBinaryFileName, String topologyDefinitionPath) {
      return action;
    }

    @Override
    public Action createRuntimeAction(Config config, ActionType type) {
      return action;
    }
  }
}
