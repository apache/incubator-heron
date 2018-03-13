//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.streamlet.scala.impl

import com.twitter.heron.streamlet.scala.{Builder, Source, Streamlet}
import com.twitter.heron.streamlet.scala.converter.ScalaToJavaConverter

class BuilderImpl(builder: com.twitter.heron.streamlet.Builder)
    extends Builder {

  override def newSource[R](supplierFn: () => R): Streamlet[R] = {
    val serializableSupplier =
      ScalaToJavaConverter.toSerializableSupplier[R](supplierFn)
    val newJavaStreamlet = builder.newSource(serializableSupplier)
    StreamletImpl.fromJavaStreamlet[R](newJavaStreamlet)
  }

  override def newSource[R](sourceFn: Source[R]): Streamlet[R] = {
    val javaSourceObj = ScalaToJavaConverter.toJavaSource[R](sourceFn)
    val newJavaStreamlet = builder.newSource(javaSourceObj)
    StreamletImpl.fromJavaStreamlet[R](newJavaStreamlet)
  }

}
