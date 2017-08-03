//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.apiserver.resources;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import com.twitter.heron.apiserver.Constants;
import com.twitter.heron.apiserver.actions.ActionFactory;
import com.twitter.heron.apiserver.actions.ActionFactoryImpl;
import com.twitter.heron.apiserver.actions.ActionType;
import com.twitter.heron.apiserver.utils.ConfigUtils;
import com.twitter.heron.apiserver.utils.FileHelper;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;

@Path("/topologies")
public class TopologyResource extends HeronResource {

  private static final String TOPOLOGY_TAR_GZ_FILENAME = "topology.tar.gz";

  private static final int HTTP_UNPROCESSABLE_ENTITY_CODE = 422;

  private static final String FORM_KEY_NAME = "name";
  private static final String FORM_KEY_CLUSTER = "cluster";
  private static final String FORM_KEY_ROLE = "role";
  private static final String FORM_KEY_ENVIRONMENT = "environment";
  private static final String FORM_KEY_DEFINITION = "definition";
  private static final String FORM_KEY_TOPOLOGY = "topology";
  private static final String FORM_KEY_USER = "user";

  private static final String[] REQUIRED_SUBMIT_TOPOLOGY_PARAMS = {
      FORM_KEY_NAME,
      FORM_KEY_CLUSTER,
      FORM_KEY_ROLE,
      FORM_KEY_DEFINITION,
      FORM_KEY_TOPOLOGY
  };

  // path format /topologies/{cluster}/{role}/{environment}/{name}
  private static final String TOPOLOGY_PATH_FORMAT = "/topologies/%s/%s/%s/%s";

  private final ActionFactory actionFactory = new ActionFactoryImpl();

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings({"IllegalCatch", "JavadocMethod"})
  public Response submit(FormDataMultiPart form) throws IOException {
    // verify that all we have all the required params
    final List<String> missingDataKeys =
        verifyKeys(form.getFields().keySet(), REQUIRED_SUBMIT_TOPOLOGY_PARAMS);
    if (!missingDataKeys.isEmpty()) {
      // return error since we are missing required parameters
      return Response.status(HTTP_UNPROCESSABLE_ENTITY_CODE)
          .type(MediaType.APPLICATION_JSON)
          .entity(createValidationError("Validation failed", missingDataKeys))
          .build();
    }

    final String topologyName = Forms.getString(form, FORM_KEY_NAME);
    final String cluster = Forms.getString(form, FORM_KEY_CLUSTER);
    final String role = Forms.getString(form, FORM_KEY_ROLE);
    final String environment =
        Forms.getString(form, FORM_KEY_ENVIRONMENT, Constants.DEFAULT_HERON_ENVIRONMENT);
    final String user = Forms.getString(form, FORM_KEY_USER, role);

    final String topologyDirectory =
        Files.createTempDirectory(topologyName).toFile().getAbsolutePath();

    try {
      // upload the topology definition file to the topology directory
      final FormDataBodyPart definitionFilePart = form.getField(FORM_KEY_DEFINITION);
      final File topologyDefinitionFile = Forms.uploadFile(definitionFilePart, topologyDirectory);

      // upload the topology binary file to the topology directory
      final FormDataBodyPart topologyFilePart = form.getField(FORM_KEY_TOPOLOGY);
      final File topologyBinaryFile = Forms.uploadFile(topologyFilePart, topologyDirectory);

      final Config config = configWithKeyValues(
          Arrays.asList(
              Pair.create(Key.CLUSTER, cluster),
              Pair.create(Key.TOPOLOGY_NAME, topologyName),
              Pair.create(Key.ROLE, role),
              Pair.create(Key.ENVIRON, environment),
              Pair.create(Key.SUBMIT_USER, user)
          )
      );

      // copy configuration files to the sandbox config location
      // topology-dir/<default-heron-sandbox-config>
      FileHelper.copyDirectory(
          Paths.get(getConfigurationDirectory()),
          Paths.get(topologyDirectory, Constants.DEFAULT_HERON_SANDBOX_CONFIG));

      // copy override file into topology configuration directory
      FileHelper.copy(Paths.get(getConfigurationOverridePath()),
          Paths.get(topologyDirectory, Constants.DEFAULT_HERON_SANDBOX_CONFIG,
              Constants.OVERRIDE_FILE));

      // apply overrides to state manager config
      ConfigUtils.applyOverridesToStateManagerConfig(
          Paths.get(topologyDirectory, Constants.DEFAULT_HERON_SANDBOX_CONFIG,
              Constants.OVERRIDE_FILE),
          Paths.get(topologyDirectory, Constants.DEFAULT_HERON_SANDBOX_CONFIG,
              Constants.STATE_MANAGER_FILE)
      );

      // create tar file from the contents of the topology directory
      final File topologyPackageFile =
          Paths.get(topologyDirectory, TOPOLOGY_TAR_GZ_FILENAME).toFile();
      FileHelper.createTarGz(topologyPackageFile, FileHelper.getChildren(topologyDirectory));

      // submit the topology
      getActionFactory()
          .createSubmitAction(config,
              topologyPackageFile.getAbsolutePath(),
              topologyBinaryFile.getName(),
              topologyDefinitionFile.getAbsolutePath())
          .execute();

      return Response.created(
          URI.create(String.format(TOPOLOGY_PATH_FORMAT,
              cluster, role, environment, topologyName)))
          .type(MediaType.APPLICATION_JSON)
          .entity(createdResponse(cluster, role, environment, topologyName)).build();
    } catch (Exception ex) {
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(createMessage(ex.getMessage()))
          .build();
    } finally {
      FileUtils.deleteDir(topologyDirectory);
    }
  }

  @POST
  @Path("/{cluster}/{role}/{environment}/{name}/activate")
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings("IllegalCatch")
  public Response activate(
      final @PathParam("cluster") String cluster,
      final @PathParam("role") String role,
      final @PathParam("environment") String environment,
      final @PathParam("name") String name) {
    try {
      final Config config = getConfig(cluster, role, environment, name);
      getActionFactory().createRuntimeAction(config, ActionType.ACTIVATE);

      return Response.ok()
          .type(MediaType.APPLICATION_JSON)
          .entity(createMessage(String.format("%s activated", name)))
          .build();
    } catch (Exception ex) {
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(createMessage(ex.getMessage()))
          .build();
    }
  }

  @POST
  @Path("/{cluster}/{role}/{environment}/{name}/deactivate")
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings("IllegalCatch")
  public Response deactivate(
      final @PathParam("cluster") String cluster,
      final @PathParam("role") String role,
      final @PathParam("environment") String environment,
      final @PathParam("name") String name) {
    try {
      final Config config = getConfig(cluster, role, environment, name);
      getActionFactory().createRuntimeAction(config, ActionType.DEACTIVATE).execute();

      return Response.ok()
          .type(MediaType.APPLICATION_JSON)
          .entity(createMessage(String.format("%s deactivated", name)))
          .build();
    } catch (Exception ex) {
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(createMessage(ex.getMessage()))
          .build();
    }
  }

  @DELETE
  @Path("/{cluster}/{role}/{environment}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings("IllegalCatch")
  public Response kill(
        final @PathParam("cluster") String cluster,
        final @PathParam("role") String role,
        final @PathParam("environment") String environment,
        final @PathParam("name") String name) {
    try {
      final Config config = getConfig(cluster, role, environment, name);
      getActionFactory().createRuntimeAction(config, ActionType.KILL).execute();

      return Response.ok()
          .type(MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      final String message = ex.getMessage();
      final Response.Status status = message.contains("does not exist")
          ? Response.Status.NOT_FOUND : Response.Status.INTERNAL_SERVER_ERROR;
      return Response.status(status)
          .type(MediaType.APPLICATION_JSON)
          .entity(createMessage(ex.getMessage()))
          .build();
    }
  }

  ActionFactory getActionFactory() {
    return actionFactory;
  }

  private Config getConfig(String cluster, String role, String environment, String topologyName) {
    return configWithKeyValues(
        Arrays.asList(
            Pair.create(Key.CLUSTER, cluster),
            Pair.create(Key.ROLE, role),
            Pair.create(Key.ENVIRON, environment),
            Pair.create(Key.TOPOLOGY_NAME, topologyName)
        ));
  }

  private Config configWithKeyValues(Collection<Pair<Key, String>> keyValues) {
    final Config.Builder builder = Config.newBuilder().putAll(getBaseConfiguration());
    for (Pair<Key, String> keyValue : keyValues) {
      builder.put(keyValue.first, keyValue.second);
    }
    builder.put(Key.VERBOSE, Boolean.TRUE);
    return Config.toLocalMode(builder.build());
  }

  private static List<String> verifyKeys(Set<String> keys, String... requiredKeys) {
    final List<String> missingKeys = new ArrayList<>();
    if (requiredKeys != null) {
      for (String key : requiredKeys) {
        if (!keys.contains(key)) {
          missingKeys.add(key);
        }
      }
    }
    return missingKeys;
  }

  private static String createdResponse(String cluster, String role, String environment,
        String topologyName) {
    return new ObjectMapper().createObjectNode()
        .put("name", topologyName)
        .put("cluster", cluster)
        .put("role", role)
        .put("environment", environment)
        .toString();
  }

  private static ObjectNode createBaseError(String message) {
    final ObjectMapper mapper = new ObjectMapper();
    return mapper.createObjectNode().put("message", message);
  }

  private static String createMessage(String message) {
    return createBaseError(message).toString();
  }

  private static String createValidationError(String message, List<String> missing) {
    ObjectNode node = createBaseError(message);
    ObjectNode errors = node.putObject("errors");
    ArrayNode missingParameters = errors.putArray("missing_parameters");
    for (String param : missing) {
      missingParameters.add(param);
    }

    return node.toString();
  }
}
