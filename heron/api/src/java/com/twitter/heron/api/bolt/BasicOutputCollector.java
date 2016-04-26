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

package com.twitter.heron.api.bolt;

import java.util.List;

import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.utils.Utils;


public class BasicOutputCollector implements IBasicOutputCollector {
  private OutputCollector mOut;
  private Tuple mInputTuple;

  public BasicOutputCollector(OutputCollector out) {
    this.mOut = out;
  }

  public List<Integer> emit(String streamId, List<Object> tuple) {
    return mOut.emit(streamId, mInputTuple, tuple);
  }

  public List<Integer> emit(List<Object> tuple) {
    return emit(Utils.DEFAULT_STREAM_ID, tuple);
  }

  public void setContext(Tuple inputTuple) {
    this.mInputTuple = inputTuple;
  }

  public void emitDirect(int taskId, String streamId, List<Object> tuple) {
    mOut.emitDirect(taskId, streamId, mInputTuple, tuple);
  }

  public void emitDirect(int taskId, List<Object> tuple) {
    emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
  }

  protected IOutputCollector getOutputter() {
    return mOut;
  }

  public void reportError(Throwable t) {
    mOut.reportError(t);
  }
}
