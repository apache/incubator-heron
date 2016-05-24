/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.storage;

import com.google.gson.Gson;

import java.nio.charset.Charset;

/**
 * Default serializer used by storm storage layer. Encode an object as json.
 */
public interface StormStoreSerializer<T> {
  /** Called to convert any object to byte array */
  byte[] serialize(T obj);

  /** Default Json serializer */
  public static class DefaultSerializer<T> implements StormStoreSerializer<T> {
    private static final String ENCODING = "UTF-8";
    private Gson gson = new Gson();

    public byte[] serialize(T obj) {
      String jsonStr = gson.toJson(obj);
      return jsonStr.getBytes(Charset.forName(ENCODING));
    }
  }
}
