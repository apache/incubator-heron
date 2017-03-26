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

/**
 * A {@link IHealthPolicy} strives to keep a distributed application healthy. It uses one or more of
 * of {@link ISymptomDetector}s, {@link IDiagnoser}s and {@link IResolver}s to achieve this. It is
 * expected that the policy will be executed periodically.
 */
public interface IHealthPolicy extends AutoCloseable {
  /**
   * Initializes this instance and should be invoked once by the system before its use.
   */
  void initialize();

  /**
   * Invoked periodically, this method orchestrates execution of {@link ISymptomDetector}s,
   * {@link IDiagnoser}s and {@link IResolver}s to keep the system healthy
   */
  void execute();

  /**
   * Release all acquired resources and prepare for termination of this instance
   */
  void close();
}
