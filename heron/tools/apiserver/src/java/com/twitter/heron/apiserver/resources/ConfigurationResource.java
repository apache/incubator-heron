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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.twitter.heron.spi.common.Config;

@Path("/config")
public class ConfigurationResource extends HeronResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response config() throws JsonProcessingException {
    final Config configuration = getBaseConfiguration();
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode();
    final Set<Map.Entry<String, Object>> sortedConfig = new TreeSet<>(keyComparator());
    sortedConfig.addAll(configuration.getEntrySet());
    for (Map.Entry<String, Object> entry : sortedConfig) {
      node.put(entry.getKey(), entry.getValue().toString());
    }

    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(mapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(node))
        .build();
  }

  private static Comparator<Map.Entry<String, Object>> keyComparator() {
    return new Comparator<Map.Entry<String, Object>>() {
      @Override
      public int compare(Map.Entry<String, Object> o1, Map.Entry<String, Object> o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    };
  }
}
