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

package org.apache.heron.classification;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

import static javax.lang.model.SourceVersion.RELEASE_11;

/**
 * Class that processes usage of {@link InterfaceAudience} and {@link InterfaceStability} at compile
 * time and emits warnings if classes are extended that shouldn't be.
 */
@SupportedAnnotationTypes({"org.apache.heron.classification.InterfaceStability.Unstable",
                           "org.apache.heron.classification.InterfaceAudience.Private",
                           "org.apache.heron.classification.InterfaceAudience.LimitedPrivate"})
@SupportedSourceVersion(RELEASE_11)
public class HeronAnnotationProcessor extends AbstractProcessor {
  private ProcessingEnvironment env;

  @Override
  public synchronized void init(ProcessingEnvironment pe) {
    this.env = pe;
  }

  /**
   * If a non-heron class extends from a class annotated as Unstable, Private or LimitedPrivate,
   * emit a warning.
   */
  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    if (!roundEnv.processingOver()) {
      for (TypeElement te : annotations) {
        for (Element elt : roundEnv.getElementsAnnotatedWith(te)) {
          if (!elt.toString().startsWith("org.apache.heron")) {
            env.getMessager().printMessage(
                Kind.WARNING,
                String.format("%s extends from a class annotated with %s", elt, te),
                elt);
          }
        }
      }
    }
    return true;
  }
}
