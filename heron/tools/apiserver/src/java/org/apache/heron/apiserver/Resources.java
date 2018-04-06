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
package org.apache.heron.apiserver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.glassfish.jersey.media.multipart.MultiPartFeature;

import org.apache.heron.apiserver.resources.ConfigurationResource;
import org.apache.heron.apiserver.resources.FileResource;
import org.apache.heron.apiserver.resources.NotFoundExceptionHandler;
import org.apache.heron.apiserver.resources.TopologyResource;

public final class Resources {

  private Resources() {
  }

  static Set<Class<?>> get() {
    return new HashSet<>(getClasses());
  }

  private static List<Class<?>> getClasses() {
    return Arrays.asList(
        ConfigurationResource.class,
        TopologyResource.class,
        NotFoundExceptionHandler.class,
        MultiPartFeature.class,
        FileResource.class
    );
  }
}
