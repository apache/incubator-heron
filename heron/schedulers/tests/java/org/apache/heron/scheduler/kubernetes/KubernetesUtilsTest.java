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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.scheduler.TopologySubmissionException;

import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesUtilsTest {

  @Test
  public void testMergeListsDedupe() {
    final String description = "Pod Template Environment Variables";
    final List<V1EnvVar> heronEnvVars =
        Collections.unmodifiableList(StatefulSet.getExecutorEnvVars());
    final V1EnvVar additionEnvVar = new V1EnvVar()
        .name("env-variable-to-be-kept")
        .valueFrom(new V1EnvVarSource()
            .fieldRef(new V1ObjectFieldSelector()
                .fieldPath("env-variable-was-kept")));
    final List<V1EnvVar> expectedEnvVars = Collections.unmodifiableList(
        new LinkedList<V1EnvVar>(StatefulSet.getExecutorEnvVars()) {{
          add(additionEnvVar);
        }});
    final List<V1EnvVar> inputEnvVars = Arrays.asList(
        new V1EnvVar()
            .name(KubernetesConstants.ENV_HOST)
            .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                    .fieldPath("env-host-to-be-replaced"))),
        new V1EnvVar()
            .name(KubernetesConstants.ENV_POD_NAME)
            .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                    .fieldPath("pod-name-to-be-replaced"))),
        additionEnvVar
    );

    KubernetesUtils.CommonUtils<V1EnvVar> commonUtils =
        new KubernetesUtils.CommonUtils<>();

    // Both input lists are null.
    Assert.assertNull("Both input lists are <null>",
         commonUtils.mergeListsDedupe(null, null,
             Comparator.comparing(V1EnvVar::getName), description));

    // <primaryList> is <null>.
    Assert.assertEquals("<primaryList> is null and <secondaryList> should be returned",
        inputEnvVars,
        commonUtils.mergeListsDedupe(null, inputEnvVars,
            Comparator.comparing(V1EnvVar::getName), description));

    // <primaryList> is empty.
    Assert.assertEquals("<primaryList> is empty and <secondaryList> should be returned",
        inputEnvVars,
        commonUtils.mergeListsDedupe(new LinkedList<>(), inputEnvVars,
            Comparator.comparing(V1EnvVar::getName), description));

    // <secondaryList> is <null>.
    Assert.assertEquals("<secondaryList> is null and <primaryList> should be returned",
        heronEnvVars,
        commonUtils.mergeListsDedupe(heronEnvVars, null,
            Comparator.comparing(V1EnvVar::getName), description));

    // <secondaryList> is empty.
    Assert.assertEquals("<secondaryList> is empty and <primaryList> should be returned",
        heronEnvVars,
        commonUtils.mergeListsDedupe(heronEnvVars, new LinkedList<>(),
            Comparator.comparing(V1EnvVar::getName), description));

    // Merge both lists.
    Assert.assertTrue("<primaryList> and <secondaryList> merged and deduplicated",
        expectedEnvVars.containsAll(
            commonUtils.mergeListsDedupe(heronEnvVars, inputEnvVars,
                Comparator.comparing(V1EnvVar::getName), description)));

    // Expect thrown error.
    String errorMessage = "";
    try {
      commonUtils.mergeListsDedupe(heronEnvVars, Collections.singletonList(new V1EnvVar()),
          Comparator.comparing(V1EnvVar::getName), description);
    } catch (TopologySubmissionException e) {
      errorMessage = e.getMessage();
    }
    Assert.assertTrue("Expecting error to be thrown for null deduplication key",
        errorMessage.contains(description));
  }
}
