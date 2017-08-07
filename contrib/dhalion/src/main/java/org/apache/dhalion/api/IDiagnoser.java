// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.apache.dhalion.api;

import org.apache.dhalion.symptom.Diagnosis;
import org.apache.dhalion.symptom.Symptom;

/**
 * A {@link IDiagnoser} evaluates one or more {@link Symptom}s and produces a {@link Diagnosis}, if
 * any, representing a possible problem responsible for the observed {@link Symptom}s.
 */
public interface IDiagnoser<T extends Symptom> extends AutoCloseable {
  /**
   * Initializes this instance and should be invoked once by the system before its use.
   */
  void initialize();

  /**
   * Evaluates available {@link Symptom}s and determines if a problem exists
   *
   * @return a {@link Diagnosis} instance representing a problem
   */
  Diagnosis<T> diagnose();

  /**
   * Release all acquired resources and prepare for termination of this instance
   */
  default void close() {
  }
}
