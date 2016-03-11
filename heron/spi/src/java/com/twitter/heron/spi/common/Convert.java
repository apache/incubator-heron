package com.twitter.heron.spi.common;

class Convert {

 protected static Integer getInteger(Object o) {
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
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to integer");
      }
    }
 }

 protected static Long getLong(Object o) {
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
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to long");
      }
    }
  }

  protected static Double getDouble(Object o) {
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
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to double");
      }
    }
  }
}
