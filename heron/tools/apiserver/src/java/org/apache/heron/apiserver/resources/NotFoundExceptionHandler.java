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

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Provider
public class NotFoundExceptionHandler implements ExceptionMapper<NotFoundException> {
  @Override
  public Response toResponse(NotFoundException exception) {
    final ObjectMapper mapper = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT)
        .enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED);
    final ObjectNode node = mapper.createObjectNode();
    final ArrayNode arrayNode = node.putArray("paths");
    arrayNode.add("/api/v1/topologies");
    arrayNode.add("/api/v1/version");
    arrayNode.add("/api/v1/file");

    final String response;
    try {
      response = mapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error creating page not found response", e);
    }

    return Response.status(Response.Status.NOT_FOUND)
        .entity(response)
        .build();
  }
}
