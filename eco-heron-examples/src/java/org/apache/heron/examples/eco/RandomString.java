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

package org.apache.heron.examples.eco;

import java.util.Random;

public class RandomString {
  private  char[] symbols;

  private Random random = new Random();

  private char[] buf;

  public RandomString(int length) {
    // Construct the symbol set
    StringBuilder tmp = new StringBuilder();
    for (char ch = '0'; ch <= '9'; ++ch) {
      tmp.append(ch);
    }

    for (char ch = 'a'; ch <= 'z'; ++ch) {
      tmp.append(ch);
    }

    symbols = tmp.toString().toCharArray();
    if (length < 1) {
      throw new IllegalArgumentException("length < 1: " + length);
    }

    buf = new char[length];
  }

  public String nextString() {
    for (int idx = 0; idx < buf.length; ++idx) {
      buf[idx] = symbols[random.nextInt(symbols.length)];
    }

    return new String(buf);
  }
}
