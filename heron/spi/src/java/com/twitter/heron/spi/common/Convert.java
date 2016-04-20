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

package com.twitter.heron.spi.common;

import java.net.URI;
import java.net.URISyntaxException;

public class Convert {

  public static Integer getInteger(Object o) {
    if (o instanceof Integer) {
      return ((Integer) o);
    } else if (o instanceof Long) {
      return new Integer(((Long) o).intValue());
    } else if (o instanceof Short) {
      return new Integer(((Short) o).intValue());
    } else {
      try {
        return Integer.parseInt(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Failed to convert " + o + " + to integer");
      }
    }
  }

  public static Long getLong(Object o) {
    if (o instanceof Long) {
      return ((Long) o);
    } else if (o instanceof Integer) {
      return new Long(((Integer) o).longValue());
    } else if (o instanceof Short) {
      return new Long(((Short) o).longValue());
    } else {
      try {
        return Long.parseLong(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Failed to convert " + o + " + to long");
      }
    }
  }

  public static Double getDouble(Object o) {
    if (o instanceof Double) {
      return ((Double) o);
    } else if (o instanceof Float) {
      return new Double(((Float) o).doubleValue());
    } else if (o instanceof Long) {
      return new Double(((Long) o).doubleValue());
    } else if (o instanceof Integer) {
      return new Double(((Integer) o).doubleValue());
    } else if (o instanceof Short) {
      return new Double(((Short) o).doubleValue());
    } else {
      try {
        return Double.parseDouble(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Failed to convert " + o + " + to double");
      }
    }
  }

  public static Boolean getBoolean(Object o) {
    if (o instanceof Boolean) {
      return ((Boolean) o);
    } else if (o instanceof String) {
      return Boolean.valueOf((String) o);
    } else {
      throw new IllegalArgumentException("Failed to convert " + o + " + to boolean");
    }
  }

  public static URI getURI(String spec) {
    try {
      return new URI(spec);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Don't know how to convert " + spec + " + to URI");
    }
  }
}
