package com.twitter.heron.common.network;

// defines some status codes
public enum StatusCode {
  OK,
  WRITE_ERROR,
  READ_ERROR,
  INVALID_PACKET,
  CONNECT_ERROR,
  CLOSE_ERROR,
  TIMEOUT_ERROR
}
