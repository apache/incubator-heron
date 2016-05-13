class ConnectionException(Exception):
  """
  Thrown when send/recv on a socket that is already closed
  """
  pass
