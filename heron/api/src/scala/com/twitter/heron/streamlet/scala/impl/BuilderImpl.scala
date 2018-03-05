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

import com.twitter.heron.streamlet.scala.{Source, Streamlet, Builder}

object BuilderImpl {

  def toScalaBuilder[R](javaBuilder: com.twitter.heron.streamlet.Builder[R]): Builder[R] =
    new BuilderImpl[R](javaBuilder)

  def toJavaBuilder[R](builder: Builder[R]): com.twitter.heron.streamlet.Builder[R] =
    builder.asInstanceOf[BuilderImpl[R]].javaBuilder

}

class BuilderImpl[R](val javaBuilder: com.twitter.heron.streamlet.Builder[R]) extends Builder[R] {

  override def newSource[R](supplierFn: ()=>Unit): Streamlet[R] = {
    val serializableSupplier = toSerializableSupplier[R](supplierFn)
    val newJavaStreamlet =
      new com.twitter.heron.streamlet.impl.streamlets.SupplierStreamlet[R](
        serializableSupplier)
    toScalaStreamlet[R](newJavaStreamlet)
  }


  override def newSource[R](sourceFn: Source[R]): Streamlet[R]} = {
    val javaSource = toJavaSource[R](sourceFn)
    javaStreamlet.toSource(javaSource)
  }

}
