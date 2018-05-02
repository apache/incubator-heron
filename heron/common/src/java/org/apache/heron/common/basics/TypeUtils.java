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

package org.apache.heron.common.basics;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;

public final class TypeUtils {

  private TypeUtils() {
  }

  public static Integer getInteger(Object o) {
    if (o instanceof Long) {
      return ((Long) o).intValue();
    } else if (o instanceof Integer) {
      return (Integer) o;
    } else if (o instanceof Short) {
      return ((Short) o).intValue();
    } else {
      try {
        return Integer.parseInt(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
      }
    }
  }

  public static Long getLong(Object o) {
    if (o instanceof Long) {
      return (Long) o;
    } else if (o instanceof Integer) {
      return ((Integer) o).longValue();
    } else if (o instanceof Short) {
      return ((Short) o).longValue();
    } else {
      try {
        return Long.parseLong(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " to long");
      }
    }
  }

  public static Double getDouble(Object o) {
    if (o instanceof Double) {
      return (Double) o;
    } else if (o instanceof Float) {
      return ((Float) o).doubleValue();
    } else if (o instanceof Long) {
      return ((Long) o).doubleValue();
    } else if (o instanceof Integer) {
      return ((Integer) o).doubleValue();
    } else if (o instanceof Short) {
      return ((Short) o).doubleValue();
    } else {
      try {
        return Double.parseDouble(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Failed to convert " + o + " to double");
      }
    }
  }

  public static Duration getDuration(Object o, TemporalUnit unit) {
    if (o != null && o instanceof Duration) {
      return (Duration) o;
    }
    try {
      return Duration.of(getLong(o), unit);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Don't know how to convert " + o + " to Duration", e);
    }
  }

  public static ByteAmount getByteAmount(Object o) {
    if (o != null && o instanceof ByteAmount) {
      return (ByteAmount) o;
    }
    try {
      return ByteAmount.fromBytes(getLong(o));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Don't know how to convert " + o + " to ByteAmount", e);
    }
  }

  public static Boolean getBoolean(Object o) {
    if (o instanceof Boolean) {
      return (Boolean) o;
    } else if (o instanceof String) {
      return Boolean.valueOf((String) o);
    } else {
      throw new IllegalArgumentException("Failed to convert " + o + " to boolean");
    }
  }

  public static URI getURI(String spec) {
    try {
      return new URI(spec);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Don't know how to convert " + spec + " to URI");
    }
  }

  @SuppressWarnings("unchecked")
  public static List<String> getListOfStrings(Object o) {
    if (o == null) {
      return new ArrayList<>();
    } else if (o instanceof List) {
      return (List<String>) o;
    } else {
      throw new IllegalArgumentException("Failed to convert " + o + " to List<String>");
    }
  }
}
