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
//  limitations under the License
package org.apache.dhalion.heron.common;

import javax.inject.Inject;

import org.apache.dhalion.api.ISymptomDetector;
import org.apache.dhalion.api.MetricsProvider;
import org.apache.dhalion.symptom.Symptom;

import com.twitter.heron.spi.common.Config;

public abstract class BaseHeronDetector<T extends Symptom> implements ISymptomDetector<T> {
  @Inject
  protected Config config;
  @Inject
  protected Config runtimeConfig;
  @Inject
  protected MetricsProvider metricsProvider;
  @Inject
  protected TopologyProvider topologyProvider;
  @Inject
  protected PackingPlanProvider packingPlanProvider;

  @Override
  public void initialize() {
  }
}
