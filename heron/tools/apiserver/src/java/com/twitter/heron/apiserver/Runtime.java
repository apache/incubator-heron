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
package com.twitter.heron.apiserver;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Runtime {

  private static final Logger LOG = LoggerFactory.getLogger(Runtime.class);

  private static final String API_BASE_PATH = "/api/v1/*";

  private Runtime() {}

  public static void main(String[] args) throws Exception {
    final ResourceConfig config = new ResourceConfig(Resources.get());
    final Server server = new Server(Constants.DEFAULT_PORT);


    final ServletContextHandler contextHandler =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    contextHandler.setContextPath("/");
    server.setHandler(contextHandler);

    final ServletHolder apiServlet =
        new ServletHolder(new ServletContainer(config));

    contextHandler.addServlet(apiServlet, API_BASE_PATH);

    try {
      server.start();

      LOG.info("Heron api server started at {}", server.getURI());

      server.join();
    } finally {
      server.destroy();
    }
  }
}
