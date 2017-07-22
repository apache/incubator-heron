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

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/topologies")
public class TopologyResource {

  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response list() {
    // TODO list topologies
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity("topologies")
        .build();
  }

  @POST
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response submit() {
    // TODO submit topology
    return Response.status(Response.Status.BAD_REQUEST).build();
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response kill(@PathParam("name") String name) {
    // TODO kill topology
    return Response.ok().entity(name).build();
  }
}
