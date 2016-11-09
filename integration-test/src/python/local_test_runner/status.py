import logging

class TestFailure(Exception):
  def __init__(self, message, error=None):
    Exception.__init__(self, message, error)
    if error:
      logging.error("%s :: %s", message, str(error))
    else:
      logging.error(message)

class TestSuccess(object):
  def __init__(self, message=None):
    if message:
      logging.info(message)
