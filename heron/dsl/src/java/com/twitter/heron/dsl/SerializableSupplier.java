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

package com.twitter.heron.dsl;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * All user supplied transformation functions have to be serializable.
 * Thus all Strealmet transformation definitions take Serializable
 * Functions as their input. We simply decorate java.util. function
 * definitions with a Serializable tag to ensure that any supplied
 * lambda functions automatically become serializable.
 */
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
