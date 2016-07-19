from heron.common.src.python.network.exceptions import ConnectionException

def send(sock, msg):
  """
  Network Buffer may be of different size, so this method ensures that the
  msg is completely sent through the socket
  """
  size = len(msg)
  totalsent = 0
  while totalsent < size:
    sent = sock.send(msg[totalsent:])
    if sent == 0:
      raise ConnectionException("Sending to socket failed")
    totalsent = totalsent + sent

  return totalsent

def recv(sock, size):
  """
  Network Buffer maybe of different size, so this method ensures that the
  'size' amount of bytes is actually read from the socket.
  """
  torecv = size
  chunks = []
  while torecv > 0:
    chunk = sock.recv(torecv)
    if chunk == '':
      raise ConnectionException("Receiving to socket failed")
    torecv = torecv - len(chunk)
    chunks.append(chunk)

  return ''.join(chunks)

