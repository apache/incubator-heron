//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at

//  http://www.apache.org/licenses/LICENSE-2.0

//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.streamlet.scala


import com.twitter.heron.streamlet.scala.impl.BuilderImpl


/**
  * Builder is used to register all sources. Builder thus keeps track
  * of all the starting points of the computation dag and uses this
  * information to build the topology
  */
object Builder {
  def newBuilder(): Builder =
    new BuilderImpl(com.twitter.heron.streamlet.Builder.newBuilder())
}

/**
  * Builder is used to register all sources. Builder thus keeps track
  * of all the starting points of the computation dag and uses this
  * information to build the topology
  */
trait Builder {

  /**
    * All sources of the computation should register using addSource.
    *
    * @param supplier The supplier function that is used to create the streamlet
    * @return a Streamlet representation of the supplier object
    */
  def newSource[R](supplierFn: () => R): Streamlet[R]


  /**
    * Creates a new Streamlet using the underlying generator
    *
    * @param generator The generator that generates the tuples of the streamlet
    * @return  a Streamlet representation of the source object
    */
  def newSource[R](sourceFn: Source[R]): Streamlet[R]
}
