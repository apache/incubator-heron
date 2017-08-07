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
package org.apache.dhalion.symptom;

import java.util.HashSet;
import java.util.Set;

/**
 * A {@link Diagnosis} instance is a representation of a possible causes of one or more
 * {@link Symptom}s. A {@link Symptom} could result in creation of one or more {@link Diagnosis}.
 * Similarly, correlated {@link Symptom}s can result in generation of a {@link Diagnosis} instance.
 */
public class Diagnosis<T extends Symptom> {
  private Set<T> symptoms;

  public Diagnosis() {
    symptoms = new HashSet<>();
  }

  public Diagnosis(Set<T> correlatedSymptoms) {
    this.symptoms = correlatedSymptoms;
  }

  public Set<T> getSymptoms() {
    return symptoms;
  }

  public void addSymptom(T item) {
    symptoms.add(item);
  }

  @Override
  public String toString() {
    return "Diagnosis{" +
        "symptom=" + symptoms +
        '}';
  }
}
