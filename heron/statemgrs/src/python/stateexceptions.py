class StateException(Exception):
  EX_TYPE_NO_NODE_ERROR = 1
  EX_TYPE_NODE_EXISTS_ERROR = 2
  EX_TYPE_NOT_EMPTY_ERROR = 3
  EX_TYPE_ZOOKEEPER_ERROR = 4
  EX_TYPE_PROTOBUF_ERROR = 5

  def __init__(self, message, exType):
    # Call the Exception constructor with
    # the message
    Exception.__init__(self, message)

    self.message = message
    self.exType = exType
