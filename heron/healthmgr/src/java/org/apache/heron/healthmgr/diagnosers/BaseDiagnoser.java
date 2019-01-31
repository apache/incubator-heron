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

package org.apache.heron.healthmgr.diagnosers;

import com.microsoft.dhalion.api.IDiagnoser;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;


public abstract class BaseDiagnoser implements IDiagnoser {
  protected ExecutionContext context;

  @Override
  public void initialize(ExecutionContext ctxt) {
    this.context = ctxt;
  }

  public enum DiagnosisType {

    DIAGNOSIS_UNDER_PROVISIONING(UnderProvisioningDiagnoser.class.getSimpleName()),
    DIAGNOSIS_SLOW_INSTANCE(SlowInstanceDiagnoser.class.getSimpleName()),
    DIAGNOSIS_DATA_SKEW(DataSkewDiagnoser.class.getSimpleName());

    private String text;

    DiagnosisType(String name) {
      this.text = name;
    }

    public String text() {
      return text;
    }

    @Override
    public String toString() {
      return text();
    }
  }
}
