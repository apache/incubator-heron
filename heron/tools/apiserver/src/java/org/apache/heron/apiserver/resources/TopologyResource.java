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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.heron.apiserver.Constants;
import org.apache.heron.apiserver.actions.ActionFactory;
import org.apache.heron.apiserver.actions.ActionFactoryImpl;
import org.apache.heron.apiserver.actions.ActionType;
import org.apache.heron.apiserver.actions.Keys;
import org.apache.heron.apiserver.utils.ConfigUtils;
import org.apache.heron.apiserver.utils.FileHelper;
import org.apache.heron.apiserver.utils.Utils;
import org.apache.heron.common.basics.DryRunFormatType;
import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.scheduler.dryrun.DryRunResponse;
import org.apache.heron.scheduler.dryrun.SubmitDryRunResponse;
import org.apache.heron.scheduler.dryrun.UpdateDryRunResponse;
import org.apache.heron.scheduler.utils.DryRunRenders;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

@Path("/topologies")
public class TopologyResource extends HeronResource {

  private static final Logger LOG = LoggerFactory.getLogger(TopologyResource.class);

  private static final String TOPOLOGY_TAR_GZ_FILENAME = "topology.tar.gz";

  private static final int HTTP_UNPROCESSABLE_ENTITY_CODE = 422;

  private static final String FORM_KEY_NAME = "name";
  private static final String FORM_KEY_CLUSTER = "cluster";
  private static final String FORM_KEY_ROLE = "role";
  private static final String FORM_KEY_ENVIRONMENT = "environment";
  private static final String FORM_KEY_DEFINITION = "definition";
  private static final String FORM_KEY_TOPOLOGY = "topology";
  private static final String FORM_KEY_USER = "user";

  private static final Set<String> SUBMIT_TOPOLOGY_PARAMS = Collections.unmodifiableSet(
      new HashSet<>(
        Arrays.asList(
            FORM_KEY_NAME,
            FORM_KEY_CLUSTER,
            FORM_KEY_ROLE,
            FORM_KEY_ENVIRONMENT,
            FORM_KEY_DEFINITION,
            FORM_KEY_TOPOLOGY,
            FORM_KEY_USER
        )
      )
  );

  private static final String[] REQUIRED_SUBMIT_TOPOLOGY_PARAMS = {
      FORM_KEY_NAME,
      FORM_KEY_CLUSTER,
      FORM_KEY_ROLE,
      FORM_KEY_DEFINITION,
      FORM_KEY_TOPOLOGY
  };

  private static final String PARAM_COMPONENT_PARALLELISM = "component_parallelism";
  private static final String PARAM_RUNTIME_CONFIG_KEY = "runtime_config";
  private static final String PARAM_DRY_RUN = "dry_run";
  private static final String PARAM_DRY_RUN_FORMAT = "dry_run_format";
  private static final String DEFAULT_DRY_RUN_FORMAT = DryRunFormatType.TABLE.toString();
  private static final String PARAM_CONTAINER_NUMBER = "container_number";

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
      final String message = String.format("Validation failed missing required params: %s",
          missingDataKeys.toString());
      return Response.status(HTTP_UNPROCESSABLE_ENTITY_CODE)
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createValidationError(message, missingDataKeys))
          .build();
    }

    final String cluster = Forms.getString(form, FORM_KEY_CLUSTER);
    if (!doesClusterMatch(cluster)) {
      return Response.status(HTTP_UNPROCESSABLE_ENTITY_CODE)
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(String.format("Unknown cluster %s expecting '%s'",
              cluster, getCluster())))
          .build();
    }

    final String topologyName = Forms.getString(form, FORM_KEY_NAME);
    final String role = Forms.getString(form, FORM_KEY_ROLE);
    final String environment =
        Forms.getString(form, FORM_KEY_ENVIRONMENT, Constants.DEFAULT_HERON_ENVIRONMENT);
    final String user = Forms.getString(form, FORM_KEY_USER, role);

    // submit overrides are passed key=value
    final Map<String, String> submitOverrides = getSubmitOverrides(form);


    final String topologyDirectory =
        Files.createTempDirectory(topologyName).toFile().getAbsolutePath();

    try {
      // upload the topology definition file to the topology directory
      final FormDataBodyPart definitionFilePart = form.getField(FORM_KEY_DEFINITION);
      final File topologyDefinitionFile = Forms.uploadFile(definitionFilePart, topologyDirectory);

      // upload the topology binary file to the topology directory
      final FormDataBodyPart topologyFilePart = form.getField(FORM_KEY_TOPOLOGY);
      final File topologyBinaryFile = Forms.uploadFile(topologyFilePart, topologyDirectory);

      final boolean isDryRun = form.getFields().containsKey(PARAM_DRY_RUN);

      final boolean isVerbose = form.getFields().containsKey("verbose");

      final boolean isVerboseGC = form.getFields().containsKey("verbose_gc");

      // copy configuration files to the sandbox config location
      // topology-dir/<default-heron-sandbox-config>
      FileHelper.copyDirectory(
          Paths.get(getConfigurationDirectory()),
          Paths.get(topologyDirectory, Constants.DEFAULT_HERON_SANDBOX_CONFIG));


      final java.nio.file.Path overridesPath =
          Paths.get(topologyDirectory, Constants.DEFAULT_HERON_SANDBOX_CONFIG,
              Constants.OVERRIDE_FILE);
      // copy override file into topology configuration directory
      FileHelper.copy(Paths.get(getConfigurationOverridePath()), overridesPath);

      // apply submit overrides
      ConfigUtils.applyOverrides(overridesPath, submitOverrides);

      // apply overrides to state manager config
      ConfigUtils.applyOverridesToStateManagerConfig(overridesPath,
          Paths.get(topologyDirectory, Constants.DEFAULT_HERON_SANDBOX_CONFIG,
              Constants.STATE_MANAGER_FILE)
      );

      // create tar file from the contents of the topology directory
      final File topologyPackageFile =
          Paths.get(topologyDirectory, TOPOLOGY_TAR_GZ_FILENAME).toFile();
      FileHelper.createTarGz(topologyPackageFile, FileHelper.getChildren(topologyDirectory));

      //create configs
      Config topologyConfig = ConfigUtils.getTopologyConfig(
          topologyPackageFile.getAbsolutePath(),
          topologyBinaryFile.getName(),
          topologyDefinitionFile.getAbsolutePath());
      List<Pair<String, Object>> val = new LinkedList<>();
      for (Map.Entry<String, Object> entry : topologyConfig.getEntrySet()) {
        val.add(Pair.create(entry.getKey(), entry.getValue()));
      }
      val.addAll(Arrays.asList(
          Pair.create(Key.CLUSTER.value(), cluster),
          Pair.create(Key.TOPOLOGY_NAME.value(), topologyName),
          Pair.create(Key.ROLE.value(), role),
          Pair.create(Key.ENVIRON.value(), environment),
          Pair.create(Key.SUBMIT_USER.value(), user),
          Pair.create(Key.DRY_RUN.value(), isDryRun),
          Pair.create(Key.VERBOSE.value(), isVerbose),
          Pair.create(Key.VERBOSE_GC.value(), isVerboseGC)
      ));
      final Config config = createConfig(val, submitOverrides);

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
    } catch (SubmitDryRunResponse response) {
      return createDryRunResponse(response,
          Forms.getString(form, PARAM_DRY_RUN_FORMAT, DEFAULT_DRY_RUN_FORMAT));
    } catch (Exception ex) {
      LOG.error("error submitting topology {}", topologyName, ex);
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(ex.getMessage()))
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
      getActionFactory().createRuntimeAction(config, ActionType.ACTIVATE).execute();

      return Response.ok()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(String.format("%s activated", name)))
          .build();
    } catch (Exception ex) {
      LOG.error("error activating topology {}", name, ex);
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(ex.getMessage()))
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
          .entity(Utils.createMessage(String.format("%s deactivated", name)))
          .build();
    } catch (Exception ex) {
      LOG.error("error deactivating topology {}", name, ex);
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(ex.getMessage()))
          .build();
    }
  }

  @POST
  @Path("/{cluster}/{role}/{environment}/{name}/restart")
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings("IllegalCatch")
  public Response restart(
      final @PathParam("cluster") String cluster,
      final @PathParam("role") String role,
      final @PathParam("environment") String environment,
      final @PathParam("name") String name,
      final @DefaultValue("-1") @FormParam("container_id") int containerId) {
    try {
      final List<Pair<String, Object>> keyValues = new ArrayList<>(
          Arrays.asList(
            Pair.create(Key.CLUSTER.value(), cluster),
            Pair.create(Key.ROLE.value(), role),
            Pair.create(Key.ENVIRON.value(), environment),
            Pair.create(Key.TOPOLOGY_NAME.value(), name),
            Pair.create(Key.TOPOLOGY_CONTAINER_ID.value(),  containerId)
          )
      );

      final Config config = createConfig(keyValues);
      getActionFactory().createRuntimeAction(config, ActionType.RESTART).execute();

      return Response.ok()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(String.format("%s restarted", name)))
          .build();
    } catch (Exception ex) {
      LOG.error("error restarting topology {}", name, ex);
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(ex.getMessage()))
          .build();
    }
  }

  @POST
  @Path("/{cluster}/{role}/{environment}/{name}/update")
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings({"IllegalCatch", "JavadocMethod"})
  public Response update(
      final @PathParam("cluster") String cluster,
      final @PathParam("role") String role,
      final @PathParam("environment") String environment,
      final @PathParam("name") String name,
      MultivaluedMap<String, String> params) {
    try {
      if (params == null) {
        return Response.status(HTTP_UNPROCESSABLE_ENTITY_CODE)
            .type(MediaType.APPLICATION_JSON)
            .entity(Utils.createMessage("no param"))
            .build();
      } else {
        List<String> components = params.get(PARAM_COMPONENT_PARALLELISM);
        List<String> runtimeConfigs = params.get(PARAM_RUNTIME_CONFIG_KEY);
        List<String> containersList = params.get(PARAM_CONTAINER_NUMBER);
        if (containersList != null && containersList.size() > 1) {
          Utils.createMessage("only one value should be specified for container_number. "
              + "picking first value.");
        }

        if (components != null && !components.isEmpty()) {
          return updateComponentParallelism(cluster, role, environment, name, params, components);
        } else if (containersList != null && !containersList.isEmpty()) {
          return updateContainerNumber(cluster, role, environment, name, params,
              containersList.get(0));
        } else if (runtimeConfigs != null && !runtimeConfigs.isEmpty()) {
          return updateRuntimeConfig(cluster, role, environment, name, params, runtimeConfigs);
        } else {
          return Response.status(HTTP_UNPROCESSABLE_ENTITY_CODE)
            .type(MediaType.APPLICATION_JSON)
            .entity(Utils.createMessage("missing component_parallelism or runtime_config param"))
            .build();
        }
      }
    } catch (UpdateDryRunResponse response) {
      return createDryRunResponse(response,
          Forms.getFirstOrDefault(params, PARAM_DRY_RUN_FORMAT, DEFAULT_DRY_RUN_FORMAT));
    } catch (Exception ex) {
      LOG.error("error updating topology {}", name, ex);
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(ex.getMessage()))
          .build();
    }
  }

  protected Response updateComponentParallelism(
      String cluster,
      String role,
      String environment,
      String name,
      MultivaluedMap<String, String> params,
      List<String> components) {

    final List<Pair<String, Object>> keyValues = new ArrayList<>(
        Arrays.asList(
            Pair.create(Key.CLUSTER.value(), cluster),
            Pair.create(Key.ROLE.value(), role),
            Pair.create(Key.ENVIRON.value(), environment),
            Pair.create(Key.TOPOLOGY_NAME.value(), name),
            Pair.create(Keys.PARAM_COMPONENT_PARALLELISM,
                String.join(",", components))
        )
    );

    // has a dry run been requested?
    if (params.containsKey(PARAM_DRY_RUN)) {
      keyValues.add(Pair.create(Key.DRY_RUN.value(), Boolean.TRUE));
    }

    final Set<Pair<String, Object>> overrides = getUpdateOverrides(params);
    // apply overrides if they exists
    if (!overrides.isEmpty()) {
      keyValues.addAll(overrides);
    }

    final Config config = createConfig(keyValues);
    getActionFactory().createRuntimeAction(config, ActionType.UPDATE).execute();

    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(Utils.createMessage(String.format("%s updated", name)))
        .build();
  }

  protected Response updateContainerNumber(
      String cluster,
      String role,
      String environment,
      String name,
      MultivaluedMap<String, String> params,
      String containerNumber) {

    final List<Pair<String, Object>> keyValues = new ArrayList<>(
        Arrays.asList(
            Pair.create(Key.CLUSTER.value(), cluster),
            Pair.create(Key.ROLE.value(), role),
            Pair.create(Key.ENVIRON.value(), environment),
            Pair.create(Key.TOPOLOGY_NAME.value(), name),
            Pair.create(Keys.PARAM_CONTAINER_NUMBER, containerNumber)
        )
    );

    // has a dry run been requested?
    if (params.containsKey(PARAM_DRY_RUN)) {
      keyValues.add(Pair.create(Key.DRY_RUN.value(), Boolean.TRUE));
    }

    final Set<Pair<String, Object>> overrides = getUpdateOverrides(params);
    // apply overrides if they exists
    if (!overrides.isEmpty()) {
      keyValues.addAll(overrides);
    }

    final Config config = createConfig(keyValues);
    getActionFactory().createRuntimeAction(config, ActionType.UPDATE).execute();

    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(Utils.createMessage(String.format("%s updated", name)))
        .build();
  }

  protected Response updateRuntimeConfig(
      String cluster,
      String role,
      String environment,
      String name,
      MultivaluedMap<String, String> params,
      List<String> runtimeConfigs) {
    // TODO(nwang): Implement runtime config in API server
    return Response.ok()
    .type(MediaType.APPLICATION_JSON)
    .entity(Utils.createMessage(String.format("%s updated", name)))
    .build();
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
          .entity(Utils.createMessage(String.format("%s killed", name)))
          .build();
    } catch (Exception ex) {
      LOG.error("error killing topology {}", name, ex);
      final String message = ex.getMessage();
      final Response.Status status = message != null && message.contains("does not exist")
          ? Response.Status.NOT_FOUND : Response.Status.INTERNAL_SERVER_ERROR;
      return Response.status(status)
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(message))
          .build();
    }
  }

  ActionFactory getActionFactory() {
    return actionFactory;
  }

  private boolean doesClusterMatch(String cluster) {
    return getCluster().equalsIgnoreCase(cluster);
  }

  static List<String> verifyKeys(Set<String> keys, String... requiredKeys) {
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

  private Config getConfig(String cluster, String role, String environment, String topologyName) {
    return createConfig(
        Arrays.asList(
            Pair.create(Key.CLUSTER.value(), cluster),
            Pair.create(Key.ROLE.value(), role),
            Pair.create(Key.ENVIRON.value(), environment),
            Pair.create(Key.TOPOLOGY_NAME.value(), topologyName)
        ));
  }

  private Config createConfig(Collection<Pair<String, Object>> keyValues) {
    return createConfig(keyValues, Collections.emptyMap());
  }

  private Config createConfig(Collection<Pair<String, Object>> keyValues,
      Map<String, String> overrides) {
    final Config.Builder builder = Config.newBuilder().putAll(getBaseConfiguration());
    for (Pair<String, Object> keyValue : keyValues) {
      builder.put(keyValue.first, keyValue.second);
    }
    overrides.forEach(builder::put);

    return isLocalMode()
        ? Config.toLocalMode(builder.build()) : Config.toClusterMode(builder.build());
  }

  private boolean isLocalMode() {
    return "local".equalsIgnoreCase(getCluster()) || "nomad".equalsIgnoreCase(getCluster());
  }

  private static Map<String, String> getSubmitOverrides(FormDataMultiPart form) {
    final Map<String, String> overrides = new HashMap<>();
    for (String key : form.getFields().keySet()) {
      if (!SUBMIT_TOPOLOGY_PARAMS.contains(key)) {
        overrides.put(key, Forms.getString(form, key));
      }
    }
    return overrides;
  }

  private static Set<Pair<String, Object>> getUpdateOverrides(
      MultivaluedMap<String, String> params) {
    final Set<Pair<String, Object>> overrides = new HashSet<>();
    for (String key : params.keySet()) {
      if (!PARAM_COMPONENT_PARALLELISM.equalsIgnoreCase(key)) {
        overrides.add(Pair.create(key, params.getFirst(key)));
      }
    }
    return overrides;
  }

  @SuppressWarnings("IllegalCatch")
  private static DryRunFormatType getDryRunFormatType(String type) {
    try {
      if (type != null) {
        return DryRunFormatType.valueOf(type);
      }
    } catch (Exception ex) {
      LOG.warn("unknown dry format render type {} defaulting to table", type);
    }
    return DryRunFormatType.TABLE;
  }

  private static String getDryRunResponse(DryRunResponse response, String type) {
    if (response instanceof SubmitDryRunResponse) {
      return DryRunRenders.render((SubmitDryRunResponse) response,
          getDryRunFormatType(type));
    } else if (response instanceof UpdateDryRunResponse) {
      return DryRunRenders.render((UpdateDryRunResponse) response,
          getDryRunFormatType(type));
    }
    return "Unknown dry run response type " + response.getClass().getName();
  }

  private static Response createDryRunResponse(DryRunResponse response, String type) {
    final String body = new ObjectMapper().createObjectNode()
        .put("response", getDryRunResponse(response, type))
        .toString();

    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(body)
        .build();
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
}
