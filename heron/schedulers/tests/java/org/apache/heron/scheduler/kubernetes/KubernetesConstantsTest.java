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

package org.apache.heron.scheduler.kubernetes;

import org.junit.Assert;
import org.junit.Test;

public class KubernetesConstantsTest {

  public static final String POD_TEMPLATE_VOLUME_NAME = "pod-template-volume";
  public static final String POD_TEMPLATE_KEY = "podspec-configmap-key";
  public static final String EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME = "pod-spec-template.yml";

  @Test
  public void testPodTemplateConfigMapConstants() {
    Assert.assertEquals(KubernetesConstants.POD_TEMPLATE_VOLUME_NAME, POD_TEMPLATE_VOLUME_NAME);
    Assert.assertEquals(KubernetesConstants.POD_TEMPLATE_KEY, POD_TEMPLATE_KEY);
    Assert.assertEquals(KubernetesConstants.EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME,
        EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME);
  }
}
