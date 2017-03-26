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
package org.apache.dhalion.api;

import java.util.Collection;

import org.apache.dhalion.symptom.Symptom;

public interface ISymptomDetector<T extends Symptom> extends AutoCloseable {
  /**
   * Initializes this instance and should be invoked once by the system before its use.
   */
  void initialize();

  /**
   * Detects a problem or issue with the distributed application
   */
  Collection<T> detect();

  /**
   * Release all acquired resources and prepare for termination of this instance
   */
  default void close() {
  }
}
