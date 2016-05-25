from random import randint

class ReqId:
  """
  32 Digit Unique Id represented as a 32 byte string
  """
  # 32 digits
  size = 32
  # smallest 32 digit number
  lower = 10**(size-1)
  # largest 32 digit number
  upper = 10**size - 1
  # zero req id
  zero = ''.zfill(size)

  @classmethod
  def generate(cls):
    return str(randint(cls.lower, cls.upper))
